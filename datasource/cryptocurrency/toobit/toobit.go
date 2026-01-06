package toobit

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

type ToobitClient struct {
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

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewToobitClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*ToobitClient, error) {
	wsEndpoint := "wss://stream.toobit.com/quote/ws/v1"

	toobit := &ToobitClient{
		name:        "toobit",
		log:         slog.Default().With(slog.String("datasource", "toobit")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Toobit allows comma-separated symbols.
	// 500 symbols per connection is safe.
	toobit.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	toobit.log.Debug("Created new datasource")
	return toobit, nil
}

func (d *ToobitClient) Connect() error {
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
	d.log.Info("Toobit datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *ToobitClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("Toobit closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *ToobitClient) IsRunning() bool {
	return d.isRunning
}

func (d *ToobitClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *ToobitClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// Toobit sends: {"topic":"realtimes", "data": [...]}
	if strings.Contains(msg, `"topic":"realtimes"`) {
		tickers, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		for _, v := range tickers {
			d.TickerTopic.Send(v)
		}
	}
}

func (d *ToobitClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	tickers := make([]model.Ticker, 0, len(event.Data))
	for _, t := range event.Data {
		symbol := model.ParseSymbol(t.Symbol)

		newTicker, err := model.NewTickerPriceString(
			t.Close,
			symbol,
			d.name,
			time.UnixMilli(t.Timestamp),
		)
		if err != nil {
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

// -------------------------------------------------------------------------
// Subscription
// -------------------------------------------------------------------------

func (d *ToobitClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Batching: "symbol": "BTCUSDT,ETHUSDT"
	batchSize := 50

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		var strBuilder strings.Builder
		for idx, v := range batch {
			strBuilder.WriteString(fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
			if idx < len(batch)-1 {
				strBuilder.WriteString(",")
			}
		}

		subMessage := map[string]interface{}{
			"topic":  "realtimes",
			"event":  "sub",
			"symbol": strBuilder.String(),
			"params": map[string]interface{}{
				"binary": false,
			},
		}

		// Throttle
		time.Sleep(50 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *ToobitClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Toobit requires {"ping": id} every 60s max. We do 20s.
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				pingMsg := map[string]interface{}{
					"ping": id,
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

func (d *ToobitClient) startWatchdog() {
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
