package kraken

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
	"github.com/hashicorp/go-multierror"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type KrakenClient struct {
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

func NewKrakenClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*KrakenClient, error) {
	wsEndpoint := "wss://ws.kraken.com/v2"

	kraken := &KrakenClient{
		name:        "kraken",
		log:         slog.Default().With(slog.String("datasource", "kraken")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.kraken.com/0",
	}

	// Kraken V2 supports reasonable batch sizes.
	// 500 symbols per connection is safe.
	kraken.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	kraken.log.Debug("Created new datasource")
	return kraken, nil
}

func (d *KrakenClient) Connect() error {
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
	d.log.Info("Kraken V2 datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *KrakenClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("Kraken closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *KrakenClient) IsRunning() bool {
	return d.isRunning
}

func (d *KrakenClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *KrakenClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// 1. Handle Pong (V2)
	if strings.Contains(msg, `"method":"pong"`) {
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 2. Handle Ticker (V2 Format)
	// {"channel":"ticker", "type":"snapshot" or "update", "data":[...]}
	if strings.Contains(msg, `"channel":"ticker"`) && strings.Contains(msg, `"data":`) {
		// Ignore subscription confirmations
		if strings.Contains(msg, `"method":"subscribe"`) {
			return
		}

		tickers, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())

		for _, t := range tickers {
			d.TickerTopic.Send(t)
		}
	}
}

func (d *KrakenClient) parseTicker(message []byte) ([]model.Ticker, error) {
	var event KrakenSnapshotUpdate
	if err := sonic.Unmarshal(message, &event); err != nil {
		return nil, err
	}

	result := make([]model.Ticker, 0, len(event.Data))

	for _, t := range event.Data {
		// Symbol from V2 is typically "BTC/USD"
		// We use your ParseSymbol helper, but we might need to rely on the Asset Mapping
		// if ParseSymbol expects standard pairs.
		// However, usually parsing "BTC/USD" works fine.
		symbol := model.ParseSymbol(t.Symbol)

		// Normalize using your KrakenAsset types if needed.
		// Since V2 usually returns clean names like "BTC/USD", this might be redundant
		// but safe to keep for "XBT" cases.
		base := KrakenAsset(symbol.Base)
		quote := KrakenAsset(symbol.Quote)

		finalSymbol := model.Symbol{
			Base:  string(base.GetStdName()),
			Quote: string(quote.GetStdName()),
		}

		// Kraken V2 ticker doesn't strictly provide a timestamp in the 'data' object
		// (it's sometimes in metadata or implicit). We use receive time.
		ts := time.Now()

		newTicker, err := model.NewTicker(
			t.Last,
			finalSymbol,
			d.name,
			ts,
		)
		if err != nil {
			continue
		}
		result = append(result, newTicker)
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Subscription & Data Fetching
// -------------------------------------------------------------------------

func (d *KrakenClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// 1. Fetch available symbols to get correct 'wsname' (e.g. "XBT/USD")
	availableMap, err := d.getAvailableSymbolsMap()
	if err != nil {
		d.log.Error("Failed to fetch available symbols", "error", err)
		return err
	}

	// 2. Filter Symbols
	var validPairs []string
	for _, req := range symbols {
		// We try to match "BASE/QUOTE" to the map
		// Kraken map keys from getAvailableSymbolsMap are normalized "BASE:QUOTE"
		key := fmt.Sprintf("%s:%s", strings.ToUpper(req.Base), strings.ToUpper(req.Quote))

		if wsName, exists := availableMap[key]; exists {
			validPairs = append(validPairs, wsName)
		}
	}

	if len(validPairs) == 0 {
		return nil
	}

	// 3. Batch Subscribe (V2)
	// V2 supports batched symbols in the params
	chunkSize := 50
	for i := 0; i < len(validPairs); i += chunkSize {
		end := i + chunkSize
		if end > len(validPairs) {
			end = len(validPairs)
		}

		batch := validPairs[i:end]
		reqID := rand.Uint32()

		subMessage := map[string]interface{}{
			"method": "subscribe",
			"req_id": reqID,
			"params": map[string]interface{}{
				"channel": "ticker",
				"symbol":  batch,
			},
		}

		time.Sleep(50 * time.Millisecond)
		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(validPairs))
	return nil
}

func (d *KrakenClient) getAvailableSymbolsMap() (map[string]string, error) {
	reqUrl := d.apiEndpoint + "/public/AssetPairs"

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

	var exchangeInfo ApiAssetPairResponse
	if err := sonic.Unmarshal(data, &exchangeInfo); err != nil {
		return nil, err
	}

	if len(exchangeInfo.Error) > 0 {
		var apierrors error
		for _, apierr := range exchangeInfo.Error {
			apierrors = multierror.Append(apierrors, errors.New(apierr))
		}
		return nil, apierrors
	}

	// Create Map: "BASE:QUOTE" -> "wsname"
	// We use the GetStdName helper to ensure we map "XXBT" -> "BTC" for our lookup key
	result := make(map[string]string)
	for _, v := range exchangeInfo.Result {
		// Use the helper to normalize the key for our lookup
		baseStd := v.Base.GetStdName()
		quoteStd := v.Quote.GetStdName()

		key := fmt.Sprintf("%s:%s", strings.ToUpper(string(baseStd)), strings.ToUpper(string(quoteStd)))

		// Map to the wsname required for subscription (e.g. "XBT/USD")
		result[key] = v.WsName
	}

	return result, nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *KrakenClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Kraken V2: {"method": "ping"}
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				pingMsg := map[string]interface{}{
					"method": "ping",
					"req_id": time.Now().UnixNano(),
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

func (d *KrakenClient) startWatchdog() {
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
