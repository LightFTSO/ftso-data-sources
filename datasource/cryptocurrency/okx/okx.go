package okx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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

type OkxClient struct {
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

func NewOkxClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*OkxClient, error) {
	wsEndpoint := "wss://ws.okx.com:8443/ws/v5/public"

	okx := &OkxClient{
		name:        "okx",
		log:         slog.Default().With(slog.String("datasource", "okx")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// OKX V5 supports large batches. 500 per connection is safe.
	okx.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	okx.log.Debug("Created new datasource")
	return okx, nil
}

func (d *OkxClient) Connect() error {
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
	d.log.Info("OKX datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *OkxClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("OKX closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *OkxClient) IsRunning() bool {
	return d.isRunning
}

func (d *OkxClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *OkxClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// OKX sends "pong" as text
	if msg == "pong" {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// OKX V5: "tickers" channel (Spot), NOT "index-tickers"
	if strings.Contains(msg, `"channel":"tickers"`) {
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

func (d *OkxClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event OkxTicker
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	tickers := make([]model.Ticker, 0, len(event.Data))
	for _, v := range event.Data {
		// InstId: "BTC-USDT"
		symbol := model.ParseSymbol(v.InstId)

		// Ts is string
		ts, err := strconv.ParseInt(v.Ts, 10, 64)
		if err != nil {
			continue
		}

		// Use "last" (Last Traded Price) for spot tickers
		// Your old code used "idxPx" (Index Price)
		price := v.Last
		if price == "" {
			price = v.Idxpx // Fallback if you really intended Index Price
		}

		newTicker, err := model.NewTickerPriceString(
			price,
			symbol,
			d.name,
			time.UnixMilli(ts),
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

func (d *OkxClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Batch args: [{"channel": "tickers", "instId": "BTC-USDT"}, ...]
	batchSize := 50

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		args := make([]map[string]string, 0, len(batch))

		for _, v := range batch {
			args = append(args, map[string]string{
				"channel": "tickers", // Changed from index-tickers to tickers (Spot)
				"instId":  fmt.Sprintf("%s-%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
			})
		}

		subMessage := map[string]interface{}{
			"op":   "subscribe",
			"args": args,
		}

		// Throttle
		time.Sleep(50 * time.Millisecond)

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

func (d *OkxClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// OKX V5 expects literal "ping" string as Text Message
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

func (d *OkxClient) startWatchdog() {
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
