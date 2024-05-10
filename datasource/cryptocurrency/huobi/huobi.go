package huobi

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	log "log/slog"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type HuobiClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol
}

func NewHuobiClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HuobiClient, error) {
	wsEndpoint := "wss://api.huobi.pro/ws"

	huobi := HuobiClient{
		name:        "huobi",
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	huobi.wsClient.SetMessageHandler(huobi.onMessage)

	log.Debug("Created new datasource", "datasource", huobi.GetName())
	return &huobi, nil
}

func (b *HuobiClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *HuobiClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected", "datasource", b.GetName())
	err = b.SubscribeTickers()
	if err != nil {
		log.Error("Error subscribing to tickers", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}

func (b *HuobiClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *HuobiClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.BinaryMessage {
		// decompress
		data, err := b.decompressGzip(message.Message)
		if err != nil {
			log.Error("Error parsing binary message", "datasource", b.GetName(), "error", err.Error())
		}

		msg := string(data)
		if strings.Contains(msg, "ping") {
			b.wsClient.SendMessage([]byte(strings.ReplaceAll(msg, "ping", "pong")))
			return nil
		}

		if (!strings.Contains(msg, "market.") && !strings.Contains(msg, ".ticker")) || !strings.Contains(msg, "open") {
			return nil
		}

		ticker, err := b.parseTicker(data)
		if err != nil {
			log.Error("Error parsing ticker", "datasource", b.GetName(),
				"ticker", ticker, "error", err.Error())
			return nil
		}
		b.TickerTopic.Send(ticker)
	}

	return nil
}

func (b *HuobiClient) parseTicker(message []byte) (*model.Ticker, error) {
	var tickerMessage HuobiTicker
	err := json.Unmarshal(message, &tickerMessage)
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
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed ticker symbol", "datasource", b.GetName(), "symbols", v.GetSymbol())
	}

	return nil
}

func (b *HuobiClient) GetName() string {
	return b.name
}

func (b *HuobiClient) decompressGzip(compressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(compressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		log.Error("Error decompressing message", "datasource", b.GetName(), "error", err.Error())
		return []byte{}, err
	}
	data, _ := io.ReadAll(r)
	r.Close()

	return data, nil
}
