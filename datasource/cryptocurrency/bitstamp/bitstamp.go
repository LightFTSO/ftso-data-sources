package bitstamp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

type BitstampClient struct {
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

func NewBitstampClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitstampClient, error) {
	wsEndpoint := "wss://ws.bitstamp.net"

	bitstamp := &BitstampClient{
		name:        "bitstamp",
		log:         slog.Default().With(slog.String("datasource", "bitstamp")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// Bitstamp handles high volume well, but we chunk to be safe.
	bitstamp.symbolChunks = symbolList.Crypto.ChunkSymbols(2048)

	bitstamp.log.Debug("Created new datasource")
	return bitstamp, nil
}

func (d *BitstampClient) Connect() error {
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

	d.startHeartbeat()
	d.startWatchdog()
	d.log.Info("Bitstamp datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *BitstampClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.log.Info("Bitstamp closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *BitstampClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitstampClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *BitstampClient) onMessage(message internal.WsMessage) {
	if message.Type != websocket.TextMessage {
		return
	}

	msg := string(message.Message)

	// Filter common events to avoid unnecessary parsing
	if strings.Contains(msg, "bts:subscription_succeeded") || strings.Contains(msg, "bts:heartbeat") {
		return
	}

	// Check for trade event
	if strings.Contains(msg, `"event":"trade"`) {
		ticker, err := d.parseTicker(message.Message)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)
	}
}

func (d *BitstampClient) parseTicker(message []byte) (model.Ticker, error) {
	var event wsTickerMessage
	if err := sonic.Unmarshal(message, &event); err != nil {
		return model.Ticker{}, err
	}

	// Channel format: "live_trades_btcusd"
	// We strip the prefix to get "btcusd"
	symbolStr := strings.TrimPrefix(event.Channel, "live_trades_")
	symbol := model.ParseSymbol(symbolStr)

	// Timestamp is string micro-seconds
	tsMicro, err := strconv.ParseInt(event.Data.TimestampMicro, 10, 64)
	if err != nil {
		return model.Ticker{}, err
	}

	return model.NewTickerPriceString(
		event.Data.LastPrice,
		symbol,
		d.name,
		time.UnixMicro(tsMicro),
	)
}

// -------------------------------------------------------------------------
// Subscription & Heartbeat
// -------------------------------------------------------------------------

func (d *BitstampClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		subMessage := map[string]interface{}{
			"event": "bts:subscribe",
			"data": map[string]interface{}{
				"channel": fmt.Sprintf("live_trades_%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			},
		}

		// Throttle slightly to prevent buffer overflow on the socket
		time.Sleep(1 * time.Millisecond)

		wsClient.TrySendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

func (d *BitstampClient) startHeartbeat() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// Bitstamp recommends a heartbeat every few seconds to keep the session alive
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					// Send Application Level Heartbeat (JSON)
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.TextMessage, // Bitstamp expects Text
						Message: []byte(`{"event":"bts:heartbeat"}`),
					})
				}
			}
		}
	}()
}

func (d *BitstampClient) startWatchdog() {
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
