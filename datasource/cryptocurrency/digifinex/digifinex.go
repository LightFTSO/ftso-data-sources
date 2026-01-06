package digifinex

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

type DigifinexClient struct {
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
	lastTimestamp  atomic.Int64 // UnixMilli
	subscriptionId atomic.Uint64
	isRunning      bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDigifinexClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*DigifinexClient, error) {
	wsEndpoint := "wss://openapi.digifinex.com/ws/v1/"

	digifinex := &DigifinexClient{
		name:        "digifinex",
		log:         slog.Default().With(slog.String("datasource", "digifinex")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://openapi.digifinex.com",
	}

	// Digifinex allows large batches (100+).
	// 500 symbols per connection is safe and efficient.
	digifinex.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	digifinex.log.Debug("Created new datasource")
	return digifinex, nil
}

func (d *DigifinexClient) Connect() error {
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
	d.log.Info("Digifinex datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *DigifinexClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("Digifinex closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *DigifinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *DigifinexClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *DigifinexClient) onMessage(message internal.WsMessage) {
	// Digifinex sends ZLIB Compressed Binary Frames
	if message.Type != websocket.BinaryMessage {
		return
	}

	// 1. Decompress
	decompressedData, err := internal.DecompressZlib(message.Message)
	if err != nil {
		d.log.Error("Error decompressing Digifinex message", "error", err)
		return
	}

	// sonic works faster on bytes, but we need string checks
	dataStr := string(decompressedData)

	// 2. Handle Pong
	// Response to server.ping: {"error": null, "result": "pong", "id": ...}
	if strings.Contains(dataStr, `"result":"pong"`) {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 3. Handle Ticker
	// {"method": "ticker.update", "params": [...]}
	if strings.Contains(dataStr, `"ticker.update"`) {
		tickers, err := d.parseTicker(decompressedData)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())

		for _, t := range tickers {
			d.TickerTopic.Send(t)
		}
	}
}

func (d *DigifinexClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	tickers := make([]model.Ticker, 0, len(event.Params))

	for _, t := range event.Params {
		// Symbol format: "BTC_USDT"
		symbol := model.ParseSymbol(t.Symbol)

		newTicker, err := model.NewTickerPriceString(
			t.LastPrice,
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
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *DigifinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch Available Symbols (O(1) Map)
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validMarkets []string
	for _, req := range symbols {
		// Digifinex Format: BASE_QUOTE (e.g. BTC_USDT)
		// API returns lower case usually, but accepts upper case in subscribe.
		// Let's normalize to Upper Case "BASE_QUOTE" for matching.
		key := fmt.Sprintf("%s_%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if _, exists := availableMap[key]; exists {
			validMarkets = append(validMarkets, key)
		}
	}

	if len(validMarkets) == 0 {
		return nil
	}

	// 3. Batch Subscribe
	// Digifinex documentation says multiple symbols allowed.
	// We'll batch 50 at a time.
	chunkSize := 50
	for i := 0; i < len(validMarkets); i += chunkSize {
		end := i + chunkSize
		if end > len(validMarkets) {
			end = len(validMarkets)
		}

		batch := validMarkets[i:end]
		id := d.subscriptionId.Add(1)

		subMessage := map[string]interface{}{
			"method": "ticker.subscribe",
			"id":     id,
			"params": batch,
		}

		// Throttle
		time.Sleep(50 * time.Millisecond)

		// Note: Digifinex uses Binary Frames for receiving, but Text Frames for sending usually work.
		// If strict binary send is required, one would marshal to bytes.
		// Most libraries support sending Text to Digifinex.
		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validMarkets))
	return nil
}

func (d *DigifinexClient) getAvailableSymbolsMap() (map[string]bool, error) {
	reqUrl := d.apiEndpoint + "/v3/markets"

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

	var marketInfo MarketInfo
	if err := sonic.Unmarshal(data, &marketInfo); err != nil {
		return nil, err
	}

	// Create Map for fast lookup
	// Digifinex returns markets like "btc_usdt" (lowercase)
	result := make(map[string]bool, len(marketInfo.Data))
	for _, m := range marketInfo.Data {
		result[strings.ToUpper(m.Market)] = true
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *DigifinexClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Digifinex requires "server.ping" every ~20s
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				// CRITICAL FIX: "method": "server.ping" (not "ping")
				pingMsg := map[string]interface{}{
					"method": "server.ping",
					"params": []string{},
					"id":     id,
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

func (d *DigifinexClient) startWatchdog() {
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
