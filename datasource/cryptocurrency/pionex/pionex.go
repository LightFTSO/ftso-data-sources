package pionex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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

type PionexClient struct {
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

func NewPionexClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*PionexClient, error) {
	pionex := &PionexClient{
		name:        "pionex",
		log:         slog.Default().With(slog.String("datasource", "pionex")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  "wss://ws.pionex.com/wsPub",
		apiEndpoint: "https://api.pionex.com/api/v1",
	}

	// 1. Fetch available symbols immediately to filter the list
	// We use a temporary context here because d.ctx isn't initialized until Connect()
	initCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	availableMap, err := pionex.getAvailableSymbolsMap(initCtx)
	if err != nil {
		// If we can't fetch symbols, we can't safely filter.
		// Depending on strictness, we could return error or fall back to full list.
		// Returning error is safer to prevent bad configurations.
		return nil, fmt.Errorf("failed to fetch pionex symbols: %w", err)
	}

	// 2. Filter the symbol list
	var validSymbols model.SymbolList
	for _, s := range symbolList.Crypto {
		key := fmt.Sprintf("%s_%s", strings.ToUpper(s.Base), strings.ToUpper(s.Quote))
		if availableMap[key] {
			validSymbols = append(validSymbols, s)
		}
	}

	// 3. Create Chunks
	// Pionex allows maximum 10 connections per IP.
	// We calculate chunk size to ensure we never exceed 10 chunks.
	maxConnections := 5
	totalSymbols := len(validSymbols)

	chunkSize := 0
	if totalSymbols > 0 {
		chunkSize = int(math.Ceil(float64(totalSymbols) / float64(maxConnections)))
	}

	if chunkSize > 0 {
		pionex.symbolChunks = validSymbols.ChunkSymbols(chunkSize)
	} else {
		pionex.symbolChunks = []model.SymbolList{}
	}

	pionex.log.Debug("Created new datasource",
		"total_symbols", len(symbolList.Crypto),
		"valid_symbols", totalSymbols,
		"chunks", len(pionex.symbolChunks))

	return pionex, nil
}

func (d *PionexClient) Connect() error {
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
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startWatchdog()
	d.log.Info("Pionex datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *PionexClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("Pionex closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *PionexClient) IsRunning() bool {
	return d.isRunning
}

func (d *PionexClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *PionexClient) onMessage(wsClient *internal.WebSocketClient, message internal.WsMessage) {
	// Respect working code: Pionex sends data in Binary Frames
	if message.Type != websocket.BinaryMessage {
		return
	}

	msg := string(message.Message)

	// 1. Handle Ping (String Replace)
	if strings.Contains(msg, "PING") {
		// "PING" -> "PONG"
		pong := strings.ReplaceAll(msg, "PING", "PONG")

		// Reply to THIS socket only
		wsClient.TrySendMessage(internal.WsMessage{
			Type:    websocket.TextMessage, // Sending Text is usually fine even if receiving Binary
			Message: []byte(pong),
		})
		return
	}

	// 2. Handle Ticker (Topic: "TRADE")
	// Exclude subscription confirmations
	if strings.Contains(msg, `"topic":"TRADE"`) && !strings.Contains(msg, "SUBSCRIBED") {
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

func (d *PionexClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event wsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	tickers := make([]model.Ticker, 0, len(event.Data))

	for _, t := range event.Data {
		// Symbol: "BTC_USDT"
		symbol := model.ParseSymbol(t.Symbol)

		newTicker, err := model.NewTickerPriceString(
			t.Price,
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

func (d *PionexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Optimization:
	// The symbols passed here are already filtered in the constructor.
	// We do NOT need to fetch available symbols again.

	for _, req := range symbols {
		// Pionex Format: BTC_USDT
		key := fmt.Sprintf("%s_%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		subMessage := map[string]interface{}{
			"op":     "SUBSCRIBE",
			"topic":  "TRADE",
			"symbol": key,
		}

		// Throttle (Pionex is sensitive to burst subscriptions)
		time.Sleep(50 * time.Millisecond)

		data, err := sonic.Marshal(subMessage)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		wsClient.SendMessage(ctx, internal.WsMessage{Type: websocket.TextMessage, Message: data, Err: nil})
		cancel()
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// Updated to accept Context so it can be called from Constructor
func (d *PionexClient) getAvailableSymbolsMap(ctx context.Context) (map[string]bool, error) {
	reqUrl := d.apiEndpoint + "/common/symbols"

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

	var resp SymbolsResponse
	if err := sonic.Unmarshal(data, &resp); err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(resp.Data.Symbols))
	for _, s := range resp.Data.Symbols {
		key := fmt.Sprintf("%s_%s", strings.ToUpper(s.BaseCurrency), strings.ToUpper(s.QuoteCurrency))
		result[key] = true
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Watchdog
// -------------------------------------------------------------------------

func (d *PionexClient) startWatchdog() {
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
