package cryptocom

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

type CryptoComClient struct {
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

func NewCryptoComClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*CryptoComClient, error) {
	wsEndpoint := "wss://stream.crypto.com/v2/market"

	cryptocom := &CryptoComClient{
		name:        "cryptocom",
		log:         slog.Default().With(slog.String("datasource", "cryptocom")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Crypto.com supports reasonable batch sizes.
	// 400 symbols per connection is safe.
	cryptocom.symbolChunks = symbolList.Crypto.ChunkSymbols(400)

	cryptocom.log.Debug("Created new datasource")
	return cryptocom, nil
}

func (d *CryptoComClient) Connect() error {
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

		// Capture client for closure use
		wsClient.SetMessageHandler(func(msg internal.WsMessage) {
			d.onMessage(wsClient, msg)
		})
		wsClient.SetLogger(d.log)

		wsClient.SetOnConnect(func() error {
			// Small sleep to allow connection settling
			time.Sleep(100 * time.Millisecond)
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startWatchdog()
	d.log.Info("Crypto.com datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *CryptoComClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Crypto.com closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *CryptoComClient) IsRunning() bool {
	return d.isRunning
}

func (d *CryptoComClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *CryptoComClient) onMessage(wsClient *internal.WebSocketClient, message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// 1. Handle Heartbeat (CRITICAL)
	// Crypto.com sends: {"id": 123, "method": "public/heartbeat"}
	// We MUST respond with: {"id": 123, "method": "public/respond-heartbeat"}
	if strings.Contains(msg, "public/heartbeat") {
		d.handleHeartbeat(wsClient, message.Message)
		return
	}

	// 2. Handle Ticker
	// {"method":"subscribe", "result":{ "channel":"ticker", "data":[...] }}
	if strings.Contains(msg, "\"channel\":\"ticker\"") && strings.Contains(msg, "\"subscription\":\"ticker.") {
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

func (d *CryptoComClient) handleHeartbeat(wsClient *internal.WebSocketClient, payload []byte) {
	var ping PublicHeartbeat
	if err := sonic.Unmarshal(payload, &ping); err != nil {
		return
	}

	// Respond with the SAME ID
	pong := PublicHeartbeat{
		Id:     ping.Id,
		Method: "public/respond-heartbeat",
	}

	wsClient.TrySendMessage(internal.WsMessage{
		Type:    websocket.TextMessage,
		Message: func() []byte { b, _ := sonic.Marshal(pong); return b }(),
	})
}

func (d *CryptoComClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	// Crypto.com returns InstrumentName in Result
	// But note: The "data" array might contain multiple updates (rare for ticker, common for trade)

	// Normalize Instrument: "BTC_USDT"
	symbol := model.ParseSymbol(event.Result.InstrumentName)

	tickers := make([]model.Ticker, 0, len(event.Result.Data))
	for _, v := range event.Result.Data {
		if v.LastPrice == "" {
			continue
		}

		newTicker, err := model.NewTickerPriceString(
			v.LastPrice,
			symbol,
			d.name,
			time.UnixMilli(v.Timestamp),
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

func (d *CryptoComClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Crypto.com accepts batch subscriptions
	batchSize := 50

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		channels := make([]string, 0, len(batch))

		for _, v := range batch {
			// Format: ticker.BTC_USDT
			channels = append(channels, fmt.Sprintf("ticker.%s_%s",
				strings.ToUpper(v.Base),
				strings.ToUpper(v.Quote)))
		}

		id := d.subscriptionId.Add(1)
		subMessage := map[string]interface{}{
			"id":     id,
			"method": "subscribe",
			"nonce":  time.Now().UnixMicro(),
			"params": map[string]interface{}{
				"channels": channels,
			},
		}

		// Small delay
		time.Sleep(50 * time.Millisecond)

		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

func (d *CryptoComClient) startWatchdog() {
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
