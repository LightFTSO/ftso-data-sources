package huobi

import (
	"bytes"
	"compress/gzip"
	"errors"
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
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger
	isRunning     bool
}

func NewHuobiClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HuobiClient, error) {
	wsEndpoint := "wss://api.huobi.pro/ws"

	huobi := HuobiClient{
		name:        "huobi",
		log:         slog.Default().With(slog.String("datasource", "huobi")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	huobi.wsClient.SetMessageHandler(huobi.onMessage)
	huobi.wsClient.SetOnConnect(huobi.onConnect)

	huobi.wsClient.SetLogger(huobi.log)
	huobi.log.Debug("Created new datasource")
	return &huobi, nil
}

func (d *HuobiClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setLastTickerWatcher()

	return nil
}

func (d *HuobiClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (d *HuobiClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *HuobiClient) IsRunning() bool {
	return d.isRunning
}

func (d *HuobiClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		// decompress
		data, err := d.decompressGzip(message.Message)
		if err != nil {
			d.log.Error("Error parsing binary message", "error", err.Error())
		}

		msg := string(data)
		if strings.Contains(msg, "ping") {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(strings.ReplaceAll(msg, "ping", "pong"))})
			return
		}

		if (!strings.Contains(msg, "market.") && !strings.Contains(msg, ".ticker")) || !strings.Contains(msg, "open") {
			return
		}

		ticker, err := d.parseTicker(data)
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", ticker, "error", err.Error())
			return
		}
		d.lastTimestamp = time.Now()
		d.TickerTopic.Send(ticker)
	}
}

func (d *HuobiClient) parseTicker(message []byte) (*model.Ticker, error) {
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
		d.GetName(),
		time.UnixMilli(tickerMessage.Timestamp))

	return ticker, err
}

func (d *HuobiClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"sub": fmt.Sprintf("market.%s%s.ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			"id":  time.Now().UnixMilli(),
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	return nil
}

func (d *HuobiClient) GetName() string {
	return d.name
}

func (d *HuobiClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(d.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				d.lastTimestamp = time.Now()
				d.wsClient.Reconnect()
			}
		}
	}()
}

func (d *HuobiClient) decompressGzip(decompressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(decompressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		d.log.Error("Error decompressing message", "error", err.Error())
		return []byte{}, err
	}
	data, _ := io.ReadAll(r)
	r.Close()

	return data, nil
}
