package xt

import (
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
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	subscriptionId atomic.Uint64
}

func NewXtClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*XtClient, error) {
	wsEndpoint := "wss://stream.xt.com/public"

	xt := XtClient{
		name:         "xt",
		log:          slog.Default().With(slog.String("datasource", "xt")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.xt.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	xt.wsClient.SetMessageHandler(xt.onMessage)

	xt.wsClient.SetLogger(xt.log)
	xt.log.Debug("Created new datasource")
	return &xt, nil
}

func (b *XtClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *XtClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	return nil
}
func (b *XtClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *XtClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"event":"ticker@`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)
		}
	}
}

func (b *XtClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		b.GetName(),
		time.UnixMilli(newTickerEvent.Data.Timestamp))

	return ticker, err
}

func (b *XtClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"id":     b.subscriptionId.Add(1),
			"method": "subscribe",
			"params": []string{fmt.Sprintf("ticker@%s_%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))},
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
		time.Sleep(5 * time.Millisecond)
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *XtClient) GetName() string {
	return b.name
}

func (b *XtClient) setLastTickerWatcher() {
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

func (b *XtClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(`ping`)})
		}
	}()
}
