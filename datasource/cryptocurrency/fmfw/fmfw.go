package fmfw

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

type FmfwClient struct {
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

func NewFmfwClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*FmfwClient, error) {
	wsEndpoint := "wss://api.fmfw.io/api/3/ws/public"

	fmfw := &FmfwClient{
		name:        "fmfw",
		log:         slog.Default().With(slog.String("datasource", "fmfw")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// FMFW handles large connections well. 1024 symbols per connection is safe.
	fmfw.symbolChunks = symbolList.Crypto.ChunkSymbols(1024)

	fmfw.log.Debug("Created new datasource")
	return fmfw, nil
}

func (d *FmfwClient) Connect() error {
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
	d.log.Info("FMFW datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *FmfwClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("FMFW closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *FmfwClient) IsRunning() bool {
	return d.isRunning
}

func (d *FmfwClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *FmfwClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// FMFW sends "ticker/price/1s" or "ticker/price/1s/batch"
	// Check for "data" to ensure it's not a confirmation message
	if strings.Contains(msg, "ticker/price") && strings.Contains(msg, `"data":`) {
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

func (d *FmfwClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	// FMFW returns a map: "ETHBTC": { ...data... }
	tickers := make([]model.Ticker, 0, len(event.Data))

	for symbolStr, data := range event.Data {
		symbol := model.ParseSymbol(symbolStr)

		newTicker, err := model.NewTickerPriceString(
			data.LastPrice,
			symbol,
			d.name,
			time.UnixMilli(data.Timestamp),
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

func (d *FmfwClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// FMFW allows batching in the "params".
	// We chunk the arguments to avoid massive JSON frames.
	batchSize := 100

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		symbolArgs := make([]string, 0, len(batch))

		for _, v := range batch {
			// FMFW format: BASEQUOTE (e.g. BTCUSD)
			symbolArgs = append(symbolArgs, fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}

		subMessage := map[string]interface{}{
			"method": "subscribe",
			"ch":     "ticker/price/1s/batch", // Use the batch channel for efficiency
			"id":     time.Now().UnixMicro(),
			"params": map[string]interface{}{
				"symbols": symbolArgs,
			},
		}

		// Throttle
		time.Sleep(20 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

func (d *FmfwClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// FMFW accepts standard Pings
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.PingMessage,
						Message: []byte{},
					})
				}
			}
		}
	}()
}

func (d *FmfwClient) startWatchdog() {
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
