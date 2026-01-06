package xt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type XtClient struct {
	name string
	log  *slog.Logger

	// Core
	TickerTopic *tickertopic.TickerTopic
	W           *sync.WaitGroup
	wsClients   []*internal.WebSocketClient

	// Config
	wsEndpoint   string
	apiEndpoint  string
	symbolChunks []model.SymbolList

	// State
	lastTimestamp  atomic.Int64 // UnixMilli
	subscriptionId atomic.Uint64
	isRunning      bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewXtClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*XtClient, error) {
	wsEndpoint := "wss://stream.xt.com/public"

	xt := &XtClient{
		name:        "xt",
		log:         slog.Default().With(slog.String("datasource", "xt")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.xt.com",
	}

	// XT handles ~500 subscriptions per connection well.
	xt.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	xt.log.Debug("Created new datasource")
	return xt, nil
}

func (d *XtClient) Connect() error {
	if d.isRunning {
		return nil
	}
	d.isRunning = true
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.W.Add(1)

	d.lastTimestamp.Store(time.Now().UnixMilli())

	for _, chunk := range d.symbolChunks {
		currentChunk := chunk
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)

		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startPingLoop()
	d.startWatchdog()
	d.log.Info("XT datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *XtClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("XT closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *XtClient) IsRunning() bool {
	return d.isRunning
}

func (d *XtClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *XtClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// XT Format: {"topic":"ticker", "event":"ticker@btc_usdt", "data":{...}}
	// Your working code checked for `"event":"ticker@`
	if strings.Contains(msg, `"event":"ticker@`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *XtClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// XT V4 structure: { "data": { "s": "btc_usdt", "c": "30000.00", "t": 1600000000000 } }
	// Assuming your WsTickerMessage struct matches the JSON
	symbol := model.ParseSymbol(event.Data.Symbol)

	return model.NewTickerPriceString(
		event.Data.LastPrice,
		symbol,
		d.name,
		time.UnixMilli(event.Data.Timestamp),
	)
}

// -------------------------------------------------------------------------
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *XtClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Batch Subscriptions
	// XT supports passing multiple params in one request: ["ticker@btc_usdt", "ticker@eth_usdt"]
	batchSize := 50

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		params := make([]string, 0, len(batch))

		for _, v := range batch {
			// XT format: ticker@base_quote (lowercase)
			params = append(params, fmt.Sprintf("ticker@%s_%s", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
		}

		id := d.subscriptionId.Add(1)
		subMessage := map[string]interface{}{
			"id":     id,
			"method": "subscribe",
			"params": params,
		}

		// Small delay to prevent frame overflow
		time.Sleep(50 * time.Millisecond)

		// Fix: Send ONLY to the specific wsClient connecting, NOT loop all clients
		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *XtClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// XT requires "ping" text message every ~20s
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		pingMsg := []byte("ping")

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.TextMessage,
						Message: pingMsg,
					})
				}
			}
		}
	}()
}

func (d *XtClient) startWatchdog() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		timeout := 30 * time.Second

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				last := d.lastTimestamp.Load()
				if time.Since(time.UnixMilli(last)) > timeout {
					d.log.Warn("Watchdog: No tickers received", "timeout", timeout.String())
					d.lastTimestamp.Store(time.Now().UnixMilli())

					for _, ws := range d.wsClients {
						ws.Reconnect()
					}
				}
			}
		}
	}()
}
