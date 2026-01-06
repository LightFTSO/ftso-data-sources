package coinbase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

type CoinbaseClient struct {
	name string
	log  *slog.Logger

	// Core
	TickerTopic *tickertopic.TickerTopic
	W           *sync.WaitGroup
	wsClients   []*internal.WebSocketClient

	// Config
	wsEndpoint   string
	symbolChunks []model.SymbolList

	// State
	lastTimestamp atomic.Int64 // UnixMilli
	isRunning     bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewCoinbaseClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*CoinbaseClient, error) {
	wsEndpoint := "wss://ws-feed.exchange.coinbase.com"

	coinbase := &CoinbaseClient{
		name:        "coinbase",
		log:         slog.Default().With(slog.String("datasource", "coinbase")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// 2048 is too large for a single subscribe frame.
	// 200 is safe and efficient.
	coinbase.symbolChunks = symbolList.Crypto.ChunkSymbols(200)

	coinbase.log.Debug("Created new datasource")
	return coinbase, nil
}

func (d *CoinbaseClient) Connect() error {
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
	d.log.Info("Coinbase datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *CoinbaseClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Coinbase closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *CoinbaseClient) IsRunning() bool {
	return d.isRunning
}

func (d *CoinbaseClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *CoinbaseClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// 1. Fast Filter: Subscriptions confirmation
	if strings.Contains(msg, `"type":"subscriptions"`) {
		// No op, just debug log if needed
		return
	}

	// 2. Handle Ticker
	if strings.Contains(msg, `"type":"ticker"`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *CoinbaseClient) parseTicker(message []byte) (model.Ticker, error) {
	var event CoinbaseTicker
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Coinbase sends "BTC-USD"
	symbol := model.ParseSymbol(event.ProductId)

	ts, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		return model.Ticker{}, err
	}

	return model.NewTickerPriceString(
		event.LastPrice,
		symbol,
		d.name,
		ts,
	)
}

// -------------------------------------------------------------------------
// Subscription & Heartbeat
// -------------------------------------------------------------------------

func (d *CoinbaseClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	productIDs := make([]string, 0, len(symbols))

	for _, v := range symbols {
		// Coinbase format: BASE-QUOTE
		pid := fmt.Sprintf("%s-%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote))
		productIDs = append(productIDs, pid)
	}

	// Batch subscribe in one go
	subMessage := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channels":    []string{"ticker"},
	}

	// Use Context-aware send
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	err := wsClient.SendMessageJSON(ctx, websocket.TextMessage, subMessage)
	if err != nil {
		d.log.Warn("Failed to send subscription", "error", err)
		return err
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(productIDs))
	return nil
}

func (d *CoinbaseClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Coinbase accepts standard Ping frames
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.PingMessage,
						Message: []byte{},
					})
				}
			}
		}
	}()
}

func (d *CoinbaseClient) startWatchdog() {
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
