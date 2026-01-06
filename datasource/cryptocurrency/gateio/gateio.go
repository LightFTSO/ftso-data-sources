package gateio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

type GateIoClient struct {
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
	lastTimestamp atomic.Int64 // UnixMilli
	isRunning     bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGateIoClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*GateIoClient, error) {
	wsEndpoint := "wss://api.gateio.ws/ws/v4/"

	gateio := &GateIoClient{
		name:        "gateio",
		log:         slog.Default().With(slog.String("datasource", "gateio")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.gateio.ws/api/v4",
	}

	// Gate.io handles reasonable volume. 500 symbols per connection.
	gateio.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	gateio.log.Debug("Created new datasource")
	return gateio, nil
}

func (d *GateIoClient) Connect() error {
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
	d.log.Info("Gate.io datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *GateIoClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("Gate.io closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *GateIoClient) IsRunning() bool {
	return d.isRunning
}

func (d *GateIoClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *GateIoClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// Gate.io sends {"channel": "spot.pong", ...}
	if strings.Contains(msg, "spot.pong") {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// Filter Tickers: channel="spot.tickers" event="update"
	if strings.Contains(msg, "spot.tickers") && strings.Contains(msg, `"update"`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *GateIoClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	symbol := model.ParseSymbol(event.Result.CurrencyPair)

	return model.NewTickerPriceString(
		event.Result.Last,
		symbol,
		d.name,
		time.UnixMilli(event.TimeMs), // Gate.io V4 sends time_ms at top level
	)
}

// -------------------------------------------------------------------------
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *GateIoClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch available symbols (O(1) Map)
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validPairs []string
	for _, req := range symbols {
		// Gate.io format: BASE_QUOTE (e.g. BTC_USDT)
		key := fmt.Sprintf("%s_%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if _, exists := availableMap[key]; exists {
			validPairs = append(validPairs, key)
		}
	}

	if len(validPairs) == 0 {
		return nil
	}

	// 3. Batch Subscribe
	// Gate.io handles reasonably sized batches. 50 is safe.
	chunkSize := 50
	for i := 0; i < len(validPairs); i += chunkSize {
		end := i + chunkSize
		if end > len(validPairs) {
			end = len(validPairs)
		}

		batch := validPairs[i:end]

		subMessage := map[string]interface{}{
			"time":    time.Now().UnixMilli(),
			"channel": "spot.tickers",
			"event":   "subscribe",
			"payload": batch,
		}

		// Small delay
		time.Sleep(50 * time.Millisecond)

		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validPairs))
	return nil
}

func (d *GateIoClient) getAvailableSymbolsMap() (map[string]bool, error) {
	reqUrl := d.apiEndpoint + "/spot/currency_pairs"

	ctx, cancel := context.WithTimeout(d.ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var pairs []GateIoInstrument
	if err := sonic.Unmarshal(data, &pairs); err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(pairs))
	for _, p := range pairs {
		// Gate.io usually returns "id": "BTC_USDT" or "base": "BTC", "quote": "USDT"
		// The struct provided had Base/Quote. Let's stick to that if verified.
		// Assuming GateIoInstrument matches: {"base": "BTC", "quote": "USDT", ...}
		key := fmt.Sprintf("%s_%s", strings.ToUpper(p.Base), strings.ToUpper(p.Quote))
		result[key] = true
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *GateIoClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Gate.io V4 expects: {"channel": "spot.ping"}
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				pingMsg := map[string]interface{}{
					"time":    time.Now().UnixMilli(),
					"channel": "spot.ping",
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

func (d *GateIoClient) startWatchdog() {
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
