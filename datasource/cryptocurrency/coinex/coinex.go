package coinex

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

type CoinexClient struct {
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

func NewCoinexClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*CoinexClient, error) {
	wsEndpoint := "wss://socket.coinex.com/v2/spot"

	coinex := &CoinexClient{
		name:        "coinex",
		log:         slog.Default().With(slog.String("datasource", "coinex")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.coinex.com/v2",
	}

	// Coinex supports larger batches. 1024 connections is excessive.
	// 2048 symbols per connection is safe.
	coinex.symbolChunks = symbolList.Crypto.ChunkSymbols(2048)

	coinex.log.Debug("Created new datasource")
	return coinex, nil
}

func (d *CoinexClient) Connect() error {
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
	d.log.Info("Coinex datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *CoinexClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Coinex closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *CoinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *CoinexClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *CoinexClient) onMessage(message internal.WsMessage) {
	var data []byte

	// 1. Decompress (Coinex sends Binary GZIP)
	if message.Type == websocket.BinaryMessage {
		decompressed, err := internal.DecompressGzip(message.Message)
		if err != nil {
			// Fail silently on bad frames
			return
		}
		data = decompressed
	} else {
		data = message.Message
	}

	msgStr := string(data)

	// 2. Handle Pong (Application Level)
	// {"error": null, "result": "pong", "id": ...}
	if strings.Contains(msgStr, `"result":"pong"`) {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 3. Handle Trades
	// {"method": "deals.update", "params": {...}}
	if strings.Contains(msgStr, `"deals.update"`) {
		tickers, err := d.parseTicker(data)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())

		for _, t := range tickers {
			d.TickerTopic.Send(t)
		}
	}
}

func (d *CoinexClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event WsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	// Coinex V2 Structure: Data is inside "params"
	// event.Params.Market = "BTCUSDT"
	symbol := model.ParseSymbol(event.Params.Market)

	tickers := make([]model.Ticker, 0, len(event.Params.DealList))

	for _, t := range event.Params.DealList {
		// "price" is string in Coinex V2
		newTicker, err := model.NewTickerPriceString(
			t.Price,
			symbol,
			d.name,
			time.UnixMilli(t.Timestamp), // Deal timestamp
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

func (d *CoinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch available symbols (O(1) Map)
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validMarkets []string
	for _, req := range symbols {
		// Coinex Format: BASEQUOTE (e.g. BTCUSDT)
		key := fmt.Sprintf("%s%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if _, exists := availableMap[key]; exists {
			validMarkets = append(validMarkets, key)
		}
	}

	if len(validMarkets) == 0 {
		return nil
	}

	// 3. Batch Subscribe
	// Coinex V2 handles larger batches fine. 50 is conservative and safe.
	chunkSize := 50
	for i := 0; i < len(validMarkets); i += chunkSize {
		end := i + chunkSize
		if end > len(validMarkets) {
			end = len(validMarkets)
		}

		batch := validMarkets[i:end]
		id := d.subscriptionId.Add(1)

		subMessage := map[string]interface{}{
			"method": "deals.subscribe",
			"id":     id,
			"params": map[string]interface{}{
				"market_list": batch,
			},
		}

		// Small delay to prevent rate limit spikes
		time.Sleep(50 * time.Millisecond)

		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validMarkets))
	return nil
}

func (d *CoinexClient) getAvailableSymbolsMap() (map[string]bool, error) {
	reqUrl := d.apiEndpoint + "/spot/ticker"

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

	// Coinex V2 HTTP Response Structure
	type CoinexTickerData struct {
		Market string `json:"market"`
	}
	type CoinexResponse struct {
		Code int                `json:"code"`
		Data []CoinexTickerData `json:"data"`
	}

	var resp CoinexResponse
	if err := sonic.Unmarshal(data, &resp); err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(resp.Data))
	for _, m := range resp.Data {
		result[m.Market] = true
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *CoinexClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Coinex V2 expects application-level ping every ~30s
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				pingMsg := map[string]interface{}{
					"method": "server.ping",
					"params": map[string]interface{}{},
					"id":     id,
				}
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessageJSON(websocket.TextMessage, pingMsg)
				}
			}
		}
	}()
}

func (d *CoinexClient) startWatchdog() {
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
