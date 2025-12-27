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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type XtClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *tickertopic.TickerTopic
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	subscriptionId atomic.Uint64

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewXtClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*XtClient, error) {
	wsEndpoint := "wss://stream.xt.com/public"

	xt := XtClient{
		name:             "xt",
		log:              slog.Default().With(slog.String("datasource", "xt")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.xt.com",
		SymbolList:       symbolList.Crypto,
		pingInterval:     20 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	xt.symbolChunks = xt.SymbolList.ChunkSymbols(1024)
	xt.log.Debug("Created new datasource")
	return &xt, nil
}

func (d *XtClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			err := d.SubscribeTickers(wsClient, chunk)
			if err != nil {
				d.log.Error("Error subscribing to tickers")
				return err
			}
			return err
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *XtClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.clientClosedChan.Send(true)
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
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

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
	ticker, err := model.NewTickerPriceString(newTickerEvent.Data.LastPrice,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.Data.Timestamp))
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *XtClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "subscribe",
			"params": []string{fmt.Sprintf("ticker@%s_%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))},
		}
		for _, wsClient := range d.wsClients {
			wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		}
		time.Sleep(5 * time.Millisecond)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))
	return nil
}

func (d *XtClient) GetName() string {
	return d.name
}

func (d *XtClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("last ticker received watcher goroutine exiting")
				return
			case <-lastTickerIntervalTimer.C:
				now := time.Now()
				d.lastTimestampMutex.Lock()
				diff := now.Sub(d.lastTimestamp)
				d.lastTimestampMutex.Unlock()

				if diff > timeout {
					// no tickers received in a while, attempt to reconnect
					d.lastTimestampMutex.Lock()
					d.lastTimestamp = time.Now()
					d.lastTimestampMutex.Unlock()

					d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))

					for _, wsClient := range d.wsClients {
						wsClient.Reconnect()
					}
				}
			}
		}
	}()
}

func (d *XtClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("ping sender goroutine exiting")
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(`ping`)})
				}
			}
		}
	}()
}
