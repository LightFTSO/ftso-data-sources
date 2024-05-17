package huobi

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type HuobiClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger
}

func NewHuobiClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HuobiClient, error) {
	wsEndpoint := "wss://api.huobi.pro/ws"

	huobi := HuobiClient{
		name:        "huobi",
		log:         slog.Default().With(slog.String("datasource", "huobi")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	huobi.wsClient.SetMessageHandler(huobi.onMessage)

	huobi.wsClient.SetLogger(huobi.log)
	huobi.log.Debug("Created new datasource")
	return &huobi, nil
}

func (b *HuobiClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.setLastTickerWatcher()

	return nil
}

func (b *HuobiClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (b *HuobiClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *HuobiClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	if message.Type == websocket.BinaryMessage {
		// decompress
		data, err := b.decompressGzip(message.Message)
		if err != nil {
			b.log.Error("Error parsing binary message", "error", err.Error())
		}

		msg := string(data)
		if strings.Contains(msg, "ping") {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(strings.ReplaceAll(msg, "ping", "pong"))})
			return
		}

		if (!strings.Contains(msg, "market.") && !strings.Contains(msg, ".ticker")) || !strings.Contains(msg, "open") {
			return
		}

		ticker, err := b.parseTicker(data)
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", ticker, "error", err.Error())
			return
		}
		b.lastTimestamp = time.Now()
		b.TickerTopic.Send(ticker)
	}
}

func (b *HuobiClient) parseTicker(message []byte) (*model.Ticker, error) {
	var tickerMessage HuobiTicker
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		return &model.Ticker{}, err
	}

	market := strings.ReplaceAll(tickerMessage.Channel, "market.", "")
	market = strings.ReplaceAll(market, ".ticker", "")

	symbol := model.ParseSymbol(market)

	ticker, err := model.NewTicker(fmt.Sprint(tickerMessage.Tick.LastPrice),
		symbol,
		b.GetName(),
		time.UnixMilli(tickerMessage.Timestamp))

	return ticker, err
}

func (b *HuobiClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"sub": fmt.Sprintf("market.%s%s.ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			"id":  time.Now().UnixMilli(),
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	return nil
}

func (b *HuobiClient) GetName() string {
	return b.name
}

func (b *HuobiClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	b.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(b.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				b.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				b.lastTimestamp = time.Now()
				b.Reconnect()
				return
			}
		}
	}()
}

func (b *HuobiClient) decompressGzip(decompressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(decompressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		b.log.Error("Error decompressing message", "error", err.Error())
		return []byte{}, err
	}
	data, _ := io.ReadAll(r)
	r.Close()

	return data, nil
}
