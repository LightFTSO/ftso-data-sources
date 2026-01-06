package bitget

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

type BitgetClient struct {
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

func NewBitgetClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitgetClient, error) {
	// Bitget V2 Public Endpoint
	wsEndpoint := "wss://ws.bitget.com/v2/ws/public"

	bitget := &BitgetClient{
		name:        "bitget",
		log:         slog.Default().With(slog.String("datasource", "bitget")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Bitget allows ~240 connections/IP, but limits subscriptions per frame.
	// 240 symbols per connection is safe if we batch the subscribe requests inside.
	bitget.symbolChunks = symbolList.Crypto.ChunkSymbols(240)

	bitget.log.Debug("Created new datasource")
	return bitget, nil
}

func (d *BitgetClient) Connect() error {
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
	d.log.Info("Bitget datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BitgetClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bitget closing...")

	d.cancel() // Stops Ping and Watchdog

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BitgetClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitgetClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BitgetClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := message.Message

	// 1. Handle "pong" response (Bitget V2 sends literal "pong")
	if string(msg) == "pong" {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 2. Fast check for Action
	// Bitget V2 Ticker channel sends "action":"snapshot" or "push"
	// We optimize by checking for the channel name first
	if !strings.Contains(string(msg), `"channel":"ticker"`) {
		// Ignore login/subscribe success messages
		return
	}

	// 3. Parse Tickers
	tickers, err := d.parseTicker(msg)
	if err != nil {
		// Log rarely
		return
	}

	if len(tickers) > 0 {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		for _, t := range tickers {
			d.TickerTopic.Send(t)
		}
	}
}

func (d *BitgetClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	// Bitget can send empty data array on initial connect sometimes
	if len(event.Data) == 0 {
		return nil, nil
	}

	// Pre-allocate slice
	tickers := make([]model.Ticker, 0, len(event.Data))

	for _, t := range event.Data {
		// InstId is usually "BTCUSDT"
		symbol := model.ParseSymbol(t.InstId)

		newTicker, err := model.NewTickerPriceString(
			t.LastPrice,
			symbol,
			d.name,
			time.UnixMilli(event.Timestamp),
		)
		if err != nil {
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

// -------------------------------------------------------------------------
// Subscription & Heartbeat
// -------------------------------------------------------------------------

func (d *BitgetClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Bitget V2 allows multiple subscriptions, but best to batch them.
	// We use packets of 10 symbols per frame to be safe and polite.
	chunkSize := 10

	for i := 0; i < len(symbols); i += chunkSize {
		end := i + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		args := make([]map[string]interface{}, 0, len(batch))

		for _, v := range batch {
			args = append(args, map[string]interface{}{
				"instType": "SPOT",
				"channel":  "ticker",
				"instId":   fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
			})
		}

		subMessage := map[string]interface{}{
			"op":   "subscribe",
			"args": args,
		}

		// Use TrySendMessage or SendMessage with context
		// Here we use a short sleep to prevent rate limiting BURSTS during reconnect
		time.Sleep(50 * time.Millisecond)

		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

func (d *BitgetClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Bitget expects "ping" every 30s. We send every 20s to be safe.
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				// Bitget V2 text-based ping
				pingMsg := internal.WsMessage{
					Type:    websocket.TextMessage,
					Message: []byte("ping"),
				}
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(pingMsg)
				}
			}
		}
	}()
}

func (d *BitgetClient) startWatchdog() {
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
