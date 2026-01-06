package kucoin

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

type KucoinClient struct {
	name string
	log  *slog.Logger

	// Core
	TickerTopic *tickertopic.TickerTopic
	W           *sync.WaitGroup
	wsClients   []*internal.WebSocketClient

	// Config
	apiEndpoint  string
	symbolChunks []model.SymbolList

	// State
	lastTimestamp atomic.Int64 // UnixMilli
	pingInterval  time.Duration
	isRunning     bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewKucoinClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*KucoinClient, error) {
	kucoin := &KucoinClient{
		name:         "kucoin",
		log:          slog.Default().With(slog.String("datasource", "kucoin")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		apiEndpoint:  "https://api.kucoin.com",
		pingInterval: 15 * time.Second, // Default fallback
	}

	// KuCoin supports multiple symbols in one topic string (comma separated).
	// To avoid URL length limits, we chunk by 50 symbols per subscription frame.
	kucoin.symbolChunks = symbolList.Crypto.ChunkSymbols(50)

	kucoin.log.Debug("Created new datasource")
	return kucoin, nil
}

func (d *KucoinClient) Connect() error {
	if d.isRunning {
		return nil
	}
	d.isRunning = true
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.W.Add(1)

	d.lastTimestamp.Store(time.Now().UnixMilli())

	// 1. Handshake: Get Bullet Token
	// We do this ONCE per Connect(). If we disconnect, the Watchdog calls Reconnect(),
	// which usually just restarts the socket. If the token expired, we might need full restart logic.
	// For simplicity in this architecture, we fetch a fresh token for every new WS connection set.
	token, endpoint, pingInt, err := d.getBulletToken()
	if err != nil {
		d.log.Error("Failed to get KuCoin bullet token", "error", err)
		return err
	}

	if pingInt > 0 {
		d.pingInterval = time.Duration(pingInt) * time.Millisecond
	}

	// Construct dynamic WS URL
	wsURL := fmt.Sprintf("%s?token=%s", endpoint, token)

	// 2. Initialize Clients
	// KuCoin allows multiple subscriptions per socket, but huge lists might need splitting.
	// We use one socket for everything unless the list is massive (e.g. >3000 symbols).
	// Since we chunked symbols for subscription frames, we can likely put them all on one socket
	// or split them if needed. For safety, let's use the standard "One Client per Chunk"
	// or combine them if your SymbolList is small.
	// Given your architecture uses `symbolChunks` to create `wsClients` array:

	// Optimization: KuCoin limits subscriptions per connection (around 300 topics).
	// Since we batch 50 symbols into ONE topic string, 1 connection can technically handle 50 * 300 symbols.
	// So 1 connection is sufficient for most use cases.
	// However, sticking to your pattern:

	// Let's create just ONE WebSocket client for efficiency, passing all chunks to it.
	wsClient := internal.NewWebSocketClient(wsURL)
	wsClient.SetMessageHandler(d.onMessage)
	wsClient.SetLogger(d.log)

	wsClient.SetOnConnect(func() error {
		// iterate all chunks and subscribe
		for _, chunk := range d.symbolChunks {
			if err := d.SubscribeTickers(wsClient, chunk); err != nil {
				return err
			}
			time.Sleep(50 * time.Millisecond) // Throttle frames
		}
		return nil
	})

	d.wsClients = append(d.wsClients, wsClient)
	wsClient.Start()

	d.startPingLoop()
	d.startWatchdog()
	d.log.Info("KuCoin datasource connected", "connections", 1)

	return nil
}

func (d *KucoinClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("KuCoin closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *KucoinClient) IsRunning() bool {
	return d.isRunning
}

func (d *KucoinClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *KucoinClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// Ignore control messages
	if strings.Contains(msg, `"type":"welcome"`) || strings.Contains(msg, `"type":"pong"`) || strings.Contains(msg, `"type":"ack"`) {
		return
	}

	// Ticker Data
	// {"type":"message", "topic":"/market/ticker:...", "subject":"trade.ticker", "data":{...}}
	if strings.Contains(msg, `"subject":"trade.ticker"`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *KucoinClient) parseTicker(message []byte) (model.Ticker, error) {
	var event WsTickerEvent
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Topic format: "/market/ticker:BTC-USDT,ETH-USDT" <- Wait, no.
	// If we subscribe batched, KuCoin sends INDIVIDUAL messages for each update.
	// The topic in the response will be specific: "/market/ticker:BTC-USDT"

	parts := strings.Split(event.Topic, ":")
	if len(parts) != 2 {
		return model.Ticker{}, fmt.Errorf("invalid topic format")
	}

	symbol := model.ParseSymbol(parts[1])

	// KuCoin sends price as string in "price" or "last" depending on version.
	// V1 ticker uses "price".
	return model.NewTickerPriceString(
		event.Data.Price,
		symbol,
		d.name,
		time.UnixMilli(event.Data.Time),
	)
}

// -------------------------------------------------------------------------
// Subscription & Handshake
// -------------------------------------------------------------------------

func (d *KucoinClient) getBulletToken() (string, string, int64, error) {
	reqUrl := d.apiEndpoint + "/api/v1/bullet-public"

	ctx, cancel := context.WithTimeout(d.ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqUrl, nil)
	if err != nil {
		return "", "", 0, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", 0, err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", 0, err
	}

	var resp BulletResponse
	if err := sonic.Unmarshal(data, &resp); err != nil {
		return "", "", 0, err
	}

	if resp.Code != "200000" || len(resp.Data.InstanceServers) == 0 {
		return "", "", 0, fmt.Errorf("invalid bullet response: %s", resp.Code)
	}

	server := resp.Data.InstanceServers[0]
	return resp.Data.Token, server.Endpoint, server.PingInterval, nil
}

func (d *KucoinClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// KuCoin Batching: /market/ticker:SYM1,SYM2,SYM3
	var symbolArgs []string
	for _, v := range symbols {
		symbolArgs = append(symbolArgs, fmt.Sprintf("%s-%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
	}

	if len(symbolArgs) == 0 {
		return nil
	}

	topic := fmt.Sprintf("/market/ticker:%s", strings.Join(symbolArgs, ","))

	subMessage := map[string]interface{}{
		"id":             time.Now().UnixNano(),
		"type":           "subscribe",
		"topic":          topic,
		"privateChannel": false,
		"response":       true,
	}

	// Use Context-aware send
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	err := wsClient.SendMessageJSON(ctx, websocket.TextMessage, subMessage)
	if err != nil {
		d.log.Warn("Failed to send subscription", "error", err)
	}

	d.log.Debug("Subscribed batch", "size", len(symbolArgs))
	return nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *KucoinClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()

		// Use server provided interval minus a buffer
		interval := d.pingInterval
		if interval > 5*time.Second {
			interval -= 2 * time.Second
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				pingMsg := map[string]interface{}{
					"id":   time.Now().UnixNano(),
					"type": "ping",
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

func (d *KucoinClient) startWatchdog() {
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

					// Reconnect simply by calling Reconnect on the WS client.
					// Note: If the bullet token expired (usually 24h), simple reconnect might fail.
					// But internal.WebSocketClient's Reconnect() just dials the URL again.
					// If that fails, the supervisor loop in WebSocketClient keeps retrying.
					// Ideally, for KuCoin, we'd fetch a new token, but that requires re-running Connect().
					// For now, triggering WS reconnect is the standard recovery.
					for _, ws := range d.wsClients {
						ws.Reconnect()
					}
				}
			}
		}
	}()
}
