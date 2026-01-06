package mexc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/mexc/pb"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type MexcClient struct {
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
	lastTimestamp  atomic.Int64 // UnixMilli
	subscriptionId atomic.Uint64
	isRunning      bool

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMexcClient(options map[string]any, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*MexcClient, error) {
	wsEndpoint := "wss://wbs-api.mexc.com/ws"

	mexc := &MexcClient{
		name:        "mexc",
		log:         slog.Default().With(slog.String("datasource", "mexc")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
	}

	// MEXC supports batch subscriptions.
	// 500 symbols per connection is safe if batched properly.
	mexc.symbolChunks = symbolList.Crypto.ChunkSymbols(500)

	mexc.log.Debug("Created new datasource")
	return mexc, nil
}

func (d *MexcClient) Connect() error {
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
	d.log.Info("MEXC datasource connected", "connections", len(d.wsClients))

	return nil
}

func (d *MexcClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Info("MEXC closing...")

	d.cancel()

	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}

	d.W.Done()
	d.isRunning = false
	return nil
}

func (d *MexcClient) IsRunning() bool {
	return d.isRunning
}

func (d *MexcClient) GetName() string {
	return d.name
}

// -------------------------------------------------------------------------
// Message Handling
// -------------------------------------------------------------------------

func (d *MexcClient) onMessage(message internal.WsMessage) {
	switch message.Type {
	case websocket.BinaryMessage:
		var newMessage pb.PushDataV3ApiWrapper
		if err := proto.Unmarshal(message.Message, &newMessage); err != nil {
			// Fail silently on bad frames
			return
		}

		// If using Protobuf wrapper, ensure we have the ticker field
		if newMessage.GetPublicMiniTicker() == nil {
			return
		}

		ticker, err := d.parseTicker(&newMessage)
		if err != nil {
			return
		}

		d.lastTimestamp.Store(time.Now().UnixMilli())
		d.TickerTopic.Send(ticker)

	case websocket.TextMessage:
		// Handle JSON PONGs or Errors if needed
	}
}

func (d *MexcClient) parseTicker(message *pb.PushDataV3ApiWrapper) (model.Ticker, error) {
	// Access via getter to be safe (handles nil)
	event := message.GetPublicMiniTicker()

	symbol := model.ParseSymbol(event.GetSymbol())

	// SendTime is usually the event generation time
	ts := time.UnixMilli(message.GetSendTime())

	return model.NewTickerPriceString(
		event.GetPrice(),
		symbol,
		d.name,
		ts,
	)
}

// -------------------------------------------------------------------------
// Subscription
// -------------------------------------------------------------------------

func (d *MexcClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// Batch subscriptions: MEXC allows list of params
	// "params": ["spot@public.miniTicker.v3.api.pb@BTCUSDT@UTC+0", ...]
	batchSize := 30

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		params := make([]string, 0, len(batch))

		for _, v := range batch {
			// Format: spot@public.miniTicker.v3.api.pb@SYMBOL@UTC+0
			topic := fmt.Sprintf("spot@public.miniTicker.v3.api.pb@%s%s@UTC+0",
				strings.ToUpper(v.Base),
				strings.ToUpper(v.Quote))
			params = append(params, topic)
		}

		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "SUBSCRIPTION",
			"params": params,
		}

		// Use Context-aware send
		ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
		err := wsClient.SendMessageJSON(ctx, websocket.TextMessage, subMessage)
		cancel()

		if err != nil {
			d.log.Warn("Failed to send subscription batch")
		}

		// Throttle
		time.Sleep(50 * time.Millisecond)
	}

	d.log.Debug("Subscribed ticker symbols", "count", len(symbols))
	return nil
}

// -------------------------------------------------------------------------
// Heartbeat & Watchdog
// -------------------------------------------------------------------------

func (d *MexcClient) startPingLoop() {
	d.W.Add(1)
	go func() {
		defer d.W.Done()
		// MEXC expects {"method": "PING"} as Text Message
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		pingMsg := []byte(`{"method":"PING"}`)

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.TrySendMessage(internal.WsMessage{
						Type:    websocket.TextMessage, // Must be Text!
						Message: pingMsg,
					})
				}
			}
		}
	}()
}

func (d *MexcClient) startWatchdog() {
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
