package bitrue

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

type BitrueClient struct {
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

func NewBitrueClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitrueClient, error) {
	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := &BitrueClient{
		name:        "bitrue",
		log:         slog.Default().With(slog.String("datasource", "bitrue")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Bitrue requires 1 subscription message per symbol.
	// 2048 is fine per connection as long as we throttle the subscribe loop slightly.
	bitrue.symbolChunks = symbolList.Crypto.ChunkSymbols(2048)

	bitrue.log.Debug("Created new datasource")
	return bitrue, nil
}

func (d *BitrueClient) Connect() error {
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

		// CRITICAL FIX: Capture wsClient in closure so we reply Pong only to THIS socket
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
	d.log.Info("Bitrue datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BitrueClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bitrue closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BitrueClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitrueClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BitrueClient) onMessage(wsClient *internal.WebSocketClient, message internal.WsMessage) {
	// Bitrue sends compressed binary GZIP data
	if message.Type != websocket.BinaryMessage {
		return
	}

	// 1. Decompress
	decompressedData, err := internal.DecompressGzip(message.Message)
	if err != nil {
		d.log.Error("Error decompressing Bitrue message", "error", err)
		return
	}

	// Convert once to string/bytes for parsing
	// Note: sonic can often unmarshal []byte directly, avoiding string alloc
	// but we do string checks first.
	dataStr := string(decompressedData)

	// 2. Handle Ping
	// Bitrue sends: {"ping": 123456789}
	if strings.Contains(dataStr, "ping") {
		// Replace "ping" -> "pong" and send back raw
		// This works because the structure is identical: {"pong": 123456789}
		pong := strings.Replace(dataStr, "ping", "pong", 1)

		// Send ONLY to the client that pinged us
		wsClient.TrySendMessage(internal.WsMessage{
			Type:    websocket.TextMessage,
			Message: []byte(pong),
		})
		return
	}

	// 3. Handle Ticker
	// Look for unique ticker identifiers
	if strings.Contains(dataStr, "_ticker") && strings.Contains(dataStr, "tick") && !strings.Contains(dataStr, "event_rep") {
		ticker, err := d.parseTicker(decompressedData)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *BitrueClient) parseTicker(message []byte) (model.Ticker, error) {
	var event TickerResponse
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Channel format: "market_btcusdt_ticker"
	// We strip the prefix/suffix to get "btcusdt"
	pair := strings.TrimSuffix(strings.TrimPrefix(event.Channel, "market_"), "_ticker")

	symbol := model.ParseSymbol(pair)

	return model.NewTicker(
		event.TickData.Close,
		symbol,
		d.name,
		time.UnixMilli(int64(event.Timestamp)),
	)
}

// -------------------------------------------------------------------------
// Subscription
// -------------------------------------------------------------------------

func (d *BitrueClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		cb_id := fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"event": "sub",
			"params": map[string]interface{}{
				"channel": fmt.Sprintf("market_%s_ticker", cb_id),
				"cb_id":   cb_id,
			},
		}

		// THROTTLE: Bitrue requires 1 frame per symbol.
		// Sending 2000 frames instantly will fill the write buffer and drop messages.
		// 1ms delay = 1000 subs/sec. Safe enough.
		// Or use a batching strategy if the API supports it (Bitrue usually doesn't).
		time.Sleep(1 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Watchdog
// -------------------------------------------------------------------------

func (d *BitrueClient) startWatchdog() {
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
