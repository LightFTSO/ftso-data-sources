package lbank

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

type LbankClient struct {
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
	lastTimestamp  atomic.Int64 // UnixMilli
	subscriptionId atomic.Uint64
	isRunning      bool
	tzInfo         *time.Location

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewLbankClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*LbankClient, error) {
	wsEndpoint := "wss://www.lbkex.net/ws/V2/"

	// LBank sends time strings in Asia/Shanghai.
	// If loading fails (e.g. minimal docker image), fallback to UTC or FixedZone.
	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		slog.Warn("Failed to load Asia/Shanghai timezone, using FixedZone +8", "error", err)
		shanghaiTimezone = time.FixedZone("CST", 8*60*60)
	}

	lbank := &LbankClient{
		name:        "lbank",
		log:         slog.Default().With(slog.String("datasource", "lbank")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		tzInfo:      shanghaiTimezone,
	}

	// LBank requires 1 frame per symbol subscription.
	// 500 connections is too many. LBank supports ~1000 subs per connection if throttled.
	// We'll chunk by 500.
	lbank.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	lbank.log.Debug("Created new datasource")
	return lbank, nil
}

func (d *LbankClient) Connect() error {
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
	d.log.Info("LBank datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *LbankClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("LBank closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *LbankClient) IsRunning() bool {
	return d.isRunning
}

func (d *LbankClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *LbankClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// LBank sends: {"type":"tick", "pair":"btc_usdt", ...}
	if strings.Contains(msg, `"type":"tick"`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *LbankClient) parseTicker(message []byte) (model.Ticker, error) {
	var event wsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	symbol := model.ParseSymbol(event.Pair)

	// LBank Format: "2024-01-01T12:00:00.123" (China Time)
	ts, err := time.ParseInLocation("2006-01-02T15:04:05.999", event.Timestamp, d.tzInfo)
	if err != nil {
		// Fallback to current time if parse fails
		ts = time.Now()
	}

	return model.NewTicker(
		event.Ticker.LastPrice,
		symbol,
		d.name,
		ts,
	)
}

// -------------------------------------------------------------------------
// Subscription
// -------------------------------------------------------------------------

func (d *LbankClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		subMessage := map[string]interface{}{
			"action":    "subscribe",
			"subscribe": "tick",
			"pair":      fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}

		// Throttle: 1ms delay prevents "write buffer full"
		time.Sleep(1 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *LbankClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// LBank expects {"action":"ping", "ping":"UUID"}
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				pingMsg := map[string]interface{}{
					"action": "ping",
					"ping":   fmt.Sprintf("%d", id),
				}
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.TextMessage,
						Message: func() []byte { b, _ := sonic.Marshal(pingMsg); return b }(),
					})
				}
			}
		}
	}()
}

func (d *LbankClient) startWatchdog() {
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
