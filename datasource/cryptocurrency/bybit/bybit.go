package bybit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
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

type BybitClient struct {
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

func NewBybitClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BybitClient, error) {
	bybit := &BybitClient{
		name:        "bybit",
		log:         slog.Default().With(slog.String("datasource", "bybit")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  "wss://stream.bybit.com/v5/public/spot",
		apiEndpoint: "https://api.bybit.com",
	}

	// Bybit connection limits are generous, but args per op are low (10).
	// We chunk by 2048 for connections, and will batch args inside Subscribe.
	bybit.symbolChunks = symbolList.Crypto.ChunkSymbols(2048)

	bybit.log.Debug("Created new datasource")
	return bybit, nil
}

func (d *BybitClient) Connect() error {
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
			// Fetches API info and subscribes
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startPingLoop()
	d.startWatchdog()
	d.log.Info("Bybit datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BybitClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bybit closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BybitClient) IsRunning() bool {
	return d.isRunning
}

func (d *BybitClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BybitClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := message.Message

	// 1. Handle Pong
	if strings.Contains(string(msg), `"op":"pong"`) {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 2. Handle Ticker
	// Bybit V5 structure: {"topic": "tickers.BTCUSDT", "data": {...}}
	if strings.Contains(string(msg), `"topic":"tickers.`) {
		ticker, err := d.parseTicker(msg)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *BybitClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Data is a struct in Bybit V5 Spot
	symbol := model.ParseSymbol(event.Data.Symbol)

	return model.NewTickerPriceString(
		event.Data.LastPrice,
		symbol,
		d.name,
		time.UnixMilli(event.Time),
	)
}

// -------------------------------------------------------------------------
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *BybitClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch Instruments Map (O(1) lookup)
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validStreams []string
	for _, req := range symbols {
		// Key format: "BASE:QUOTE" (Normalized)
		key := fmt.Sprintf("%s:%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if target, exists := availableMap[key]; exists {
			// Bybit topic: "tickers.BTCUSDT"
			stream := fmt.Sprintf("tickers.%s%s", target.BaseCoin, target.QuoteCoin)
			validStreams = append(validStreams, stream)
		}
	}

	if len(validStreams) == 0 {
		return nil
	}

	// 3. Batch Subscriptions
	// Bybit limit: 10 args per request
	chunkSize := 10
	for i := 0; i < len(validStreams); i += chunkSize {
		end := i + chunkSize
		if end > len(validStreams) {
			end = len(validStreams)
		}

		batch := validStreams[i:end]
		reqID := strconv.FormatUint(rand.Uint64(), 36)

		subMessage := map[string]interface{}{
			"op":     "subscribe",
			"req_id": reqID,
			"args":   batch,
		}

		// Small delay to prevent rate limit spikes
		time.Sleep(20 * time.Millisecond)

		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to subscribe batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validStreams))
	return nil
}

func (d *BybitClient) getAvailableSymbolsMap() (map[string]BybitSymbol, error) {
	reqUrl := d.apiEndpoint + "/v5/market/instruments-info?category=spot"

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

	var response InstrumentInfoResponse
	if err := sonic.Unmarshal(data, &response); err != nil {
		return nil, err
	}

	// Convert to Map for O(1) matching
	result := make(map[string]BybitSymbol)
	for _, item := range response.Result.List {
		// Key: "BTC:USDT"
		key := fmt.Sprintf("%s:%s", strings.ToUpper(item.BaseCoin), strings.ToUpper(item.QuoteCoin))
		result[key] = item
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *BybitClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Bybit V5 requires: {"op": "ping"} every 20s
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				pingMsg := internal.WsMessage{
					Type:    websocket.TextMessage, // Must be Text, not PingMessage
					Message: []byte(`{"op":"ping"}`),
				}
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(pingMsg)
				}
			}
		}
	}()
}

func (d *BybitClient) startWatchdog() {
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
