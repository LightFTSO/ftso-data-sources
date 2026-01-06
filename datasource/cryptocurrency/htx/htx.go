package htx

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

type HTXClient struct {
	name string
	log  *slog.Logger

	// Core
	TickerTopic *tickertopic.TickerTopic
	W           *sync.WaitGroup
	wsClients   []*internal.WebSocketClient

	// Config
	wsEndpoint   string
	symbolChunks []model.SymbolList

	// State
	lastTimestamp atomic.Int64 // UnixMilli
	isRunning     bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewHTXClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*HTXClient, error) {
	// Updated to HTX Endpoint
	wsEndpoint := "wss://api.htx.com/ws"

	htx := &HTXClient{
		name:        "htx", // Keeping internal name "htx" for config compatibility
		log:         slog.Default().With(slog.String("datasource", "htx")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// HTX handles ~1000 subs per connection reasonably well if throttled.
	htx.symbolChunks = symbolList.Crypto.ChunkSymbols(1024)

	htx.log.Debug("Created new datasource")
	return htx, nil
}

func (d *HTXClient) Connect() error {
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

		// Capture client for Ping replies
		wsClient.SetMessageHandler(func(msg internal.WsMessage) {
			d.onMessage(wsClient, msg)
		})
		wsClient.SetLogger(d.log)

		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startWatchdog()
	d.log.Info("HTX (HTX) datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *HTXClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("HTX closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *HTXClient) IsRunning() bool {
	return d.isRunning
}

func (d *HTXClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *HTXClient) onMessage(wsClient *internal.WebSocketClient, message internal.WsMessage) {
	// HTX sends binary GZIP frames for everything
	if message.Type != websocket.BinaryMessage {
		return
	}

	// 1. Decompress
	decompressed, err := internal.DecompressGzip(message.Message)
	if err != nil {
		d.log.Error("Gzip error", "err", err)
		return
	}

	// 2. Check for Ping (Server-initiated)
	// HTX Format: {"ping": 123456789}
	// We do a fast string check first
	msgStr := string(decompressed)
	if strings.Contains(msgStr, "ping") {
		d.handlePing(wsClient, decompressed)
		return
	}

	if strings.Contains(msgStr, "subbed") || strings.Contains(msgStr, "err-msg") {
		return
	}

	// 3. Handle Ticker
	// Format: {"ch": "market.btcusdt.ticker", "ts": ..., "tick": {...}}
	if strings.Contains(msgStr, "market.") && strings.Contains(msgStr, ".ticker") {
		ticker, err := d.parseTicker(decompressed)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *HTXClient) handlePing(wsClient *internal.WebSocketClient, data []byte) {
	var pingMsg struct {
		Ping int64 `json:"ping"`
	}
	if err := sonic.Unmarshal(data, &pingMsg); err != nil {
		return
	}

	// Must verify it's actually a ping (Ping > 0)
	if pingMsg.Ping > 0 {
		pongMsg := map[string]interface{}{
			"pong": pingMsg.Ping,
		}

		// Reply immediately to the same socket
		wsClient.TrySendMessage(internal.WsMessage{
			Type:    websocket.TextMessage,
			Message: func() []byte { b, _ := sonic.Marshal(pongMsg); return b }(),
		})
	}
}

func (d *HTXClient) parseTicker(message []byte) (model.Ticker, error) {
	var event HTXTicker
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Channel: "market.btcusdt.ticker"
	// Strip parts to get "btcusdt"
	market := strings.TrimSuffix(strings.TrimPrefix(event.Channel, "market."), ".ticker")

	symbol := model.ParseSymbol(market)

	return model.NewTicker(
		event.Tick.LastPrice,
		symbol,
		d.name,
		time.UnixMilli(event.Timestamp),
	)
}

// -------------------------------------------------------------------------
// Subscription
// -------------------------------------------------------------------------

func (d *HTXClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		// HTX Format: market.btcusdt.ticker
		ch := fmt.Sprintf("market.%s%s.ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"sub": ch,
			"id":  fmt.Sprintf("%d", time.Now().UnixNano()),
		}

		// Throttle: 1ms delay prevents "write buffer full" on large lists
		time.Sleep(1 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Watchdog
// -------------------------------------------------------------------------

func (d *HTXClient) startWatchdog() {
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
					d.log.Warn("Watchdog: No tickers received", "timeout", timeout)
					d.lastTimestamp.Store(time.Now().UnixMilli())

					for _, ws := range d.wsClients {
						ws.Reconnect()
					}
				}
			}
		}
	}()
}
