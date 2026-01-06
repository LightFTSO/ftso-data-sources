package whitebit

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

type WhitebitClient struct {
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

func NewWhitebitClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*WhitebitClient, error) {
	wsEndpoint := "wss://api.whitebit.com/ws"

	whitebit := &WhitebitClient{
		name:        "whitebit",
		log:         slog.Default().With(slog.String("datasource", "whitebit")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://whitebit.com/api/v4/",
	}

	// WhiteBIT handles batches well. 500 symbols per connection is safe.
	whitebit.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	whitebit.log.Debug("Created new datasource")
	return whitebit, nil
}

func (d *WhitebitClient) Connect() error {
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
	d.log.Info("WhiteBIT datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *WhitebitClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("WhiteBIT closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *WhitebitClient) IsRunning() bool {
	return d.isRunning
}

func (d *WhitebitClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *WhitebitClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// WhiteBIT notifications: {"method": "lastprice_update", "params": ["BTC_USDT", "50000.00"]}
	if strings.Contains(msg, "lastprice_update") {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *WhitebitClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	if len(event.Params) < 2 {
		return model.Ticker{}, errors.New("invalid params")
	}

	// Params[0] is Symbol ("BTC_USDT")
	symbol := model.ParseSymbol(event.Params[0])

	// Params[1] is Price String ("50000.00")
	priceStr := event.Params[1]

	return model.NewTickerPriceString(
		priceStr,
		symbol,
		d.name,
		time.Now(), // WhiteBIT "lastprice" feed does not provide timestamp
	)
}

// -------------------------------------------------------------------------
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *WhitebitClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch available symbols (O(1) Map)
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validMarkets []string
	for _, req := range symbols {
		// WhiteBIT format: BASE_QUOTE (e.g. BTC_USDT)
		// Try to match uppercase
		key := fmt.Sprintf("%s_%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if _, exists := availableMap[key]; exists {
			validMarkets = append(validMarkets, key)
		}
	}

	if len(validMarkets) == 0 {
		return nil
	}

	// 3. Batch Subscribe
	// WhiteBIT is lenient, but 100 per batch is safe practice.
	batchSize := 100

	for i := 0; i < len(validMarkets); i += batchSize {
		end := i + batchSize
		if end > len(validMarkets) {
			end = len(validMarkets)
		}

		batch := validMarkets[i:end]
		id := d.subscriptionId.Add(1)

		subMessage := map[string]interface{}{
			"id":     id,
			"method": "lastprice_subscribe",
			"params": batch,
		}

		// Throttle
		time.Sleep(50 * time.Millisecond)

		// Fix: Send ONLY to the specific wsClient, not loop all clients
		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validMarkets))
	return nil
}

func (d *WhitebitClient) getAvailableSymbolsMap() (map[string]bool, error) {
	reqUrl := d.apiEndpoint + "public/markets"

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

	var markets []WhitebitMarketPair
	if err := sonic.Unmarshal(data, &markets); err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(markets))
	for _, m := range markets {
		// m.Name is like "BTC_USDT"
		result[m.Name] = true
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *WhitebitClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				pingMsg := map[string]interface{}{
					"id":     id,
					"method": "ping",
					"params": []string{},
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

func (d *WhitebitClient) startWatchdog() {
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
