package binance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
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

type BinanceClient struct {
	name string
	log  *slog.Logger

	// Core
	TickerTopic *tickertopic.TickerTopic
	W           *sync.WaitGroup
	wsClients   []*internal.WebSocketClient

	// Config
	wsEndpoint   string
	apiEndpoint  string
	symbolChunks []model.SymbolList // Static config chunks

	// State
	lastTimestamp atomic.Int64 // UnixMilli (Atomic is lighter than Mutex)
	isRunning     bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewBinanceClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.com:443/stream?streams="

	binance := &BinanceClient{
		name:        "binance",
		log:         slog.Default().With(slog.String("datasource", "binance")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.binance.com",
	}

	// Chunk symbols at startup (static config)
	binance.symbolChunks = symbolList.Crypto.ChunkSymbols(1024)

	binance.log.Debug("Created new datasource")
	return binance, nil
}

func (d *BinanceClient) Connect() error {
	if d.isRunning {
		return nil
	}
	d.isRunning = true

	// Create a fresh context for this connection session
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.W.Add(1)

	// Initialize timestamp to now so watchdog doesn't fire immediately
	d.lastTimestamp.Store(time.Now().UnixMilli())

	for _, chunk := range d.symbolChunks {
		// Capture chunk for closure
		currentChunk := chunk

		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)

		wsClient.SetOnConnect(func() error {
			// INSTRUCTION FOLLOWED: This runs on every reconnect.
			// It will fetch HTTP info to validate/discover pairs in the currentChunk.
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startWatchdog()
	d.log.Info("Binance datasource connected")

	return nil
}

func (d *BinanceClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}

	d.log.Info("Binance closing...")

	// 1. Cancel context (Stops Watchdog immediately)
	d.cancel()

	// 2. Close WebSockets
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BinanceClient) IsRunning() bool {
	return d.isRunning
}

func (d *BinanceClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BinanceClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		// Fast check before parsing
		if strings.Contains(string(message.Message), "@ticker") {
			ticker, err := d.parseTicker(message.Message)
			if err != nil {
				// Log once in a while or debug to save CPU on spammy errors
				d.log.Debug("Error parsing ticker", "error", err)
				return
			}

			// OPTIMIZATION: Atomic store (No Mutex lock overhead)
			d.lastTimestamp.Store(time.Now().UnixMilli())

			d.TickerTopic.Send(ticker)
		}
	}
}

func (d *BinanceClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerMessage
	err := sonic.Unmarshal(message, &event)
	if err != nil {
		return model.Ticker{}, err
	}

	symbol := model.ParseSymbol(event.Data.Symbol)
	return model.NewTickerPriceString(
		event.Data.LastPrice,
		symbol,
		d.name,
		time.UnixMilli(event.Data.Time),
	)
}

// -------------------------------------------------------------------------
// Dynamic Subscription Logic
// -------------------------------------------------------------------------

func (d *BinanceClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch Exchange Info (HTTP) dynamically on every connect
	availableSymbols, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Error obtaining available symbols", "error", err)
		// We don't return error here to avoid killing the WS connection loop.
		// We just won't subscribe this time, but the connection stays alive to retry.
		return err
	}

	// 2. Filter requested symbols against available ones
	// OPTIMIZATION: Map lookup O(N) instead of Nested Loop O(N*M)
	var validStreams []string

	for _, req := range symbols {
		// Check if Base matches (using the map key)
		// Note: Binance API returns upper case, we need to match carefully
		// Ideally, map keys should be "BASE:QUOTE" or similar to be precise,
		// but matching your logic of checking Base & Quote separately:

		// To replicate your exact logic efficiently, we can iterate:
		// But since we want O(N), let's assume getAvailableSymbolsMap returns a map of "BASE/QUOTE" -> Struct

		// Let's stick to the map logic:
		key := fmt.Sprintf("%s/%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if target, exists := availableSymbols[key]; exists {
			// Found it! Use the official formatting from the API response
			streamName := fmt.Sprintf("%s%s@ticker",
				strings.ToLower(target.BaseAsset),
				strings.ToLower(target.QuoteAsset))
			validStreams = append(validStreams, streamName)
		}
	}

	if len(validStreams) == 0 {
		d.log.Warn("No valid symbols found to subscribe in this chunk")
		return nil
	}

	// 3. Send Subscription
	subMessage := map[string]interface{}{
		"method": "SUBSCRIBE",
		"id":     rand.Uint32(), // Simple random ID
		"params": validStreams,
	}

	// Use Context-aware send (5s timeout)
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	err = wsClient.SendMessageJSON(ctx, websocket.TextMessage, subMessage)
	if err == nil {
		d.log.Debug("Subscribed ticker symbols", "count", len(validStreams))
	}
	return err
}

// getAvailableSymbolsMap returns a map for O(1) lookups: "BASE/QUOTE" -> SymbolInfo
func (d *BinanceClient) getAvailableSymbolsMap() (map[string]BinanceSymbol, error) {
	reqUrl := d.apiEndpoint + "/api/v3/exchangeInfo?permissions=SPOT"

	// Use a context with timeout for the HTTP request
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

	var exchangeInfo BinanceExchangeInfoResponse
	err = sonic.Unmarshal(data, &exchangeInfo)
	if err != nil {
		return nil, err
	}

	// Convert to Map for fast lookup
	symbolMap := make(map[string]BinanceSymbol, len(exchangeInfo.Symbols))
	for _, s := range exchangeInfo.Symbols {
		key := fmt.Sprintf("%s/%s", strings.ToUpper(s.BaseAsset), strings.ToUpper(s.QuoteAsset))
		symbolMap[key] = s
	}

	return symbolMap, nil
}

// -------------------------------------------------------------------------
// Optimized Watchdog
// -------------------------------------------------------------------------

func (d *BinanceClient) startWatchdog() {
	d.W.Add(1) // Keep WaitGroup aligned

	go func() {
		defer d.W.Done()

		// OPTIMIZATION: Check every 5 seconds instead of 1.
		// If timeout is 30s, 1s precision is overkill and wastes CPU.
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		timeout := 30 * time.Second

		for {
			select {
			case <-d.ctx.Done():
				d.log.Debug("Watchdog exiting")
				return

			case <-ticker.C:
				// Atomic Load (Cheap)
				last := d.lastTimestamp.Load()
				lastTime := time.UnixMilli(last)

				if time.Since(lastTime) > timeout {
					// We are stale.
					// Update timestamp to prevent spamming reconnects if it takes time
					d.lastTimestamp.Store(time.Now().UnixMilli())

					d.log.Warn("Watchdog: No tickers received", "timeout", timeout.String())

					// Trigger reconnect on all clients
					for _, wsClient := range d.wsClients {
						wsClient.Reconnect()
					}
				}
			}
		}
	}()
}
