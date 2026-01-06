package bitmart

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

type BitmartClient struct {
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

func NewBitmartClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitmartClient, error) {
	wsEndpoint := "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"

	bitmart := &BitmartClient{
		name:        "bitmart",
		log:         slog.Default().With(slog.String("datasource", "bitmart")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Bitmart recommended limit is ~50-100 subs per connection/frame
	bitmart.symbolChunks = symbolList.Crypto.ChunkSymbols(100)

	bitmart.log.Debug("Created new datasource")
	return bitmart, nil
}

func (d *BitmartClient) Connect() error {
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
	d.log.Info("Bitmart datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BitmartClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bitmart closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BitmartClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitmartClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BitmartClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// 1. Handle Pong
	if msg == "pong" {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 2. Filter Subscription Confirmations
	if strings.Contains(msg, `"event":"subscribe"`) {
		return
	}

	// 3. Process Ticker Data
	// Bitmart sends: {"table":"spot/ticker", "data":[...]}
	if strings.Contains(msg, `"table":"spot/ticker"`) {
		tickers, err := d.parseTicker(message.Message)
		if err != nil {
			// Rate limit logs here if needed
			return
		}

		if len(tickers) > 0 {
			d.lastTimestamp.Store(time.Now().UnixMilli())
			for _, v := range tickers {
				d.TickerTopic.Send(v)
			}
		}
	}
}

func (d *BitmartClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	tickers := make([]model.Ticker, 0, len(event.Data))

	for _, t := range event.Data {
		// Bitmart symbol format: "BTC_USDT"
		// We normalize inside ParseSymbol
		symbol := model.ParseSymbol(t.Symbol)

		newTicker, err := model.NewTickerPriceString(
			t.LastPrice,
			symbol,
			d.name,
			time.UnixMilli(t.TimestampMs),
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

func (d *BitmartClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Bitmart likes smaller batches inside the op
	batchSize := 10

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		args := make([]string, 0, len(batch))

		for _, v := range batch {
			// Format: "spot/ticker:BTC_USDT"
			args = append(args, fmt.Sprintf("spot/ticker:%s_%s",
				strings.ToUpper(v.Base),
				strings.ToUpper(v.Quote)))
		}

		subMessage := map[string]interface{}{
			"op":   "subscribe",
			"args": args,
		}

		// Small delay to be polite
		time.Sleep(20 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

func (d *BitmartClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Bitmart requires periodic ping string
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
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

func (d *BitmartClient) startWatchdog() {
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
