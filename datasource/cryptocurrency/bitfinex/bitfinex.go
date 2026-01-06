package bitfinex

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

type BitfinexClient struct {
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
	lastTimestamp    atomic.Int64 // UnixMilli
	channelSymbolMap sync.Map     // Maps int(ChannelID) -> string(Symbol)
	isRunning        bool

	// Mappings for Bitfinex quirks (UST -> USDT)
	apiToNormalMap map[string]string
	normalToApiMap map[string]string

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewBitfinexClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitfinexClient, error) {
	// 1. Setup Symbol Mappings (O(1) Lookup)
	// Bitfinex uses weird names like "UST" for Tether
	apiMap := map[string]string{
		"UST": "USDT",
		"TSD": "TUSD",
	}
	// Reverse map for subscription
	normMap := make(map[string]string, len(apiMap))
	for k, v := range apiMap {
		normMap[v] = k
	}

	bitfinex := &BitfinexClient{
		name:           "bitfinex",
		log:            slog.Default().With(slog.String("datasource", "bitfinex")),
		W:              w,
		TickerTopic:    tickerTopic,
		wsClients:      []*internal.WebSocketClient{},
		wsEndpoint:     "wss://api-pub.bitfinex.com/ws/2",
		apiEndpoint:    "https://api-pub.bitfinex.com",
		apiToNormalMap: apiMap,
		normalToApiMap: normMap,
	}

	// 2. Fetch Available Symbols Once (Blocking HTTP call at startup)
	// This ensures we only try to chunk symbols that actually exist.
	available, err := bitfinex.fetchAvailableSymbols()
	if err != nil {
		bitfinex.log.Error("Failed to fetch Bitfinex symbols", "error", err)
		return nil, err
	}

	// 3. Filter & Chunk
	// We intersect the user's requested config with what Bitfinex actually supports
	var validSymbols model.SymbolList
	for _, req := range symbolList.Crypto {
		for _, av := range available {
			if strings.EqualFold(req.Base, av.Base) && strings.EqualFold(req.Quote, av.Quote) {
				validSymbols = append(validSymbols, av)
				break
			}
		}
	}

	// Bitfinex requires individual sub messages, so chunk size isn't strictly limited by frame size,
	// but 50 per connection is a safe balance for the connection limit.
	bitfinex.symbolChunks = validSymbols.ChunkSymbols(50)

	bitfinex.log.Debug("Created new datasource", "valid_symbols", len(validSymbols))
	return bitfinex, nil
}

func (d *BitfinexClient) Connect() error {
	if d.isRunning {
		return nil
	}
	d.isRunning = true
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.W.Add(1)

	d.lastTimestamp.Store(time.Now().UnixMilli())

	for _, chunk := range d.symbolChunks {
		currentChunk := chunk // Capture for closure

		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)

		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, currentChunk)
		})

		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.startWatchdog()
	d.log.Info("Bitfinex datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BitfinexClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bitfinex closing...")

	d.cancel() // Stop watchdog

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BitfinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitfinexClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BitfinexClient) onMessage(message internal.WsMessage) {
	// Bitfinex sends everything as Text (JSON)
	data := message.Message

	// 1. Fast Path: Check for Array (Data) vs Object (Event)
	// Data starts with '[', Events start with '{'
	if len(data) == 0 {
		return
	}

	if data[0] == '{' {
		// Parse Event (subscribed, info, error)
		d.handleEventMessage(data)
		return
	}

	// 2. Data Message: [CHAN_ID, ...]
	// We do a "Dirty Parse" first to check for "hb" (Heartbeat) before full unmarshal
	if strings.Contains(string(data), "\"hb\"") {
		// Heartbeat received - Keep connection alive
		d.lastTimestamp.Store(time.Now().UnixMilli())
		return
	}

	// 3. Trade Parsing
	// Bitfinex sends: [CHAN_ID, "te", [ID, TIME, AMOUNT, PRICE]]
	// We check for "te" (Trade Executed) which is the low-latency feed
	if !strings.Contains(string(data), "\"te\"") {
		return
	}

	ticker, err := d.parseTicker(data)
	if err != nil {
		// Log rarely to avoid spam
		return
	}

	d.lastTimestamp.Store(time.Now().UnixMilli())
	d.TickerTopic.Send(ticker)

}

func (d *BitfinexClient) handleEventMessage(data []byte) {
	// We only really care about "subscribed" to map IDs
	if !strings.Contains(string(data), "subscribed") {
		return
	}

	var subMsg SubscribeMessage
	if err := sonic.Unmarshal(data, &subMsg); err != nil {
		return
	}

	if subMsg.Event == "subscribed" {
		// Store Mapping: 1234 -> "BTCUSD"
		// Symbol comes in as "tBTCUSD", we strip "t"
		normalized := d.mapApiToNormal(strings.TrimPrefix(subMsg.Pair, "t"))
		d.channelSymbolMap.Store(subMsg.ChannelId, normalized)
	}
}

func (d *BitfinexClient) parseTicker(message []byte) (model.Ticker, error) {
	// Structure: [ CHAN_ID, "te", [ ID, MTS, AMOUNT, PRICE ] ]
	var event []interface{}
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	if len(event) < 3 {
		return model.Ticker{}, errors.New("malformed trade data types")
	}

	// 1. Resolve Symbol from Channel ID
	chanID, ok := event[0].(float64)
	if !ok {
		return model.Ticker{}, errors.New("malformed trade data types")
	}

	symRaw, ok := d.channelSymbolMap.Load(int(chanID))
	if !ok {
		// Received data for a channel we haven't confirmed subscription for yet?
		return model.Ticker{}, errors.New("malformed trade data types")
	}
	symbolStr := symRaw.(string)

	// 2. Extract Trade Data
	// event[2] is the array of trade details
	tradeData, ok := event[2].([]interface{})
	if !ok || len(tradeData) < 4 {
		return model.Ticker{}, errors.New("malformed trade data types")
	}

	// Index 1: MTS (Millisecond Timestamp)
	// Index 3: Price
	tsVal, ok1 := tradeData[1].(float64)
	priceVal, ok2 := tradeData[3].(float64)

	if !ok1 || !ok2 {
		return model.Ticker{}, errors.New("malformed trade data types")
	}

	return model.NewTicker(
		priceVal,
		model.ParseSymbol(symbolStr), // This is cached/fast usually
		d.name,
		time.UnixMilli(int64(tsVal)),
	)
}

// -------------------------------------------------------------------------
// Subscription & Setup
// -------------------------------------------------------------------------

func (d *BitfinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, s := range symbols {
		// Bitfinex format: "t" + BASE + QUOTE (e.g., tBTCUSD)
		// We need to handle the mapping (e.g. USDT -> UST)
		base := d.mapNormalToApi(s.Base)
		quote := d.mapNormalToApi(s.Quote)

		pair := fmt.Sprintf("t%s%s", base, quote)
		if len(base) > 3 || len(quote) > 3 {
			pair = fmt.Sprintf("t%s:%s", base, quote)
		}

		subMessage := map[string]interface{}{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  pair,
		}

		// Fire and forget (with buffer check)
		// We don't wait for confirmation here; we handle it in onMessage
		err := wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
		if err != nil {
			d.log.Warn("Failed to send subscription", "pair", pair)
		}
	}

	d.log.Debug("Sent subscription requests", "count", len(symbols))
	return nil
}

// fetchAvailableSymbols hits the HTTP API to get the canonical list
func (d *BitfinexClient) fetchAvailableSymbols() (model.SymbolList, error) {
	// v2/conf/pub:list:pair:exchange returns [ [ "BTCUSD", "ETHUSD", ... ] ]
	reqUrl := d.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	var response [][]string
	if err := sonic.Unmarshal(data, &response); err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, errors.New("empty symbol response from bitfinex")
	}

	var list model.SymbolList
	for _, rawPair := range response[0] {
		// Raw pairs are usually "BTCUSD" or "TESTBTC:TESTUSD"
		// We need to guess split if it's 6 chars, or split by ':'

		// Note: Robust splitting of "BTCUSD" without a separator is hard if tokens vary in length.
		// Bitfinex convention: 3+3. If longer, they usually use ':'
		// For safety, we rely on our known SymbolList to match against.

		// Simplified parse: assume 3:3 split for standard pairs
		// This is "Good Enough" for finding matches against our config
		normalized := d.mapApiToNormal(rawPair)
		list = append(list, model.ParseSymbol(normalized))
	}

	return list, nil
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func (d *BitfinexClient) mapApiToNormal(s string) string {
	// e.g. "BTCUST" -> "BTCUSDT"
	for k, v := range d.apiToNormalMap {
		s = strings.Replace(s, k, v, -1)
	}
	return s
}

func (d *BitfinexClient) mapNormalToApi(s string) string {
	// e.g. "USDT" -> "UST"
	// Optimization: Direct map lookup
	if val, ok := d.normalToApiMap[s]; ok {
		return val
	}
	return s
}

func (d *BitfinexClient) startWatchdog() {
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
					d.log.Warn("Watchdog: No trades received", "timeout", timeout)
					d.lastTimestamp.Store(time.Now().UnixMilli()) // Prevent spam

					for _, ws := range d.wsClients {
						ws.Reconnect()
					}
				}
			}
		}
	}()
}
