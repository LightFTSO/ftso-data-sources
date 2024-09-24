package xt

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type XtClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	subscriptionId atomic.Uint64

	isRunning bool
}

func NewXtClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*XtClient, error) {
	wsEndpoint := "wss://stream.xt.com/public"

	xt := XtClient{
		name:         "xt",
		log:          slog.Default().With(slog.String("datasource", "xt")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.xt.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	xt.wsClient.SetMessageHandler(xt.onMessage)
	xt.wsClient.SetOnConnect(xt.onConnect)

	xt.wsClient.SetLogger(xt.log)
	xt.log.Debug("Created new datasource")
	return &xt, nil
}

func (d *XtClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *XtClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (d *XtClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *XtClient) IsRunning() bool {
	return d.isRunning
}

func (d *XtClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"event":"ticker@`) {
			ticker, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			d.lastTimestamp = time.Now()
			d.TickerTopic.Send(ticker)
		}
	}
}

func (d *XtClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.Data.Timestamp))

	return ticker, err
}

func (d *XtClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "subscribe",
			"params": []string{fmt.Sprintf("ticker@%s_%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))},
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
		time.Sleep(5 * time.Millisecond)
	}

	d.log.Debug("Subscribed ticker symbols")

	return nil
}

func (d *XtClient) GetName() string {
	return d.name
}

func (d *XtClient) setLastTickerWatcher() {
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

func (d *XtClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(`ping`)})
		}
	}()
}
