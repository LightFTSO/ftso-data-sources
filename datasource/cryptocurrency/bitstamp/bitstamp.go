package bitstamp

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BitstampClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewBitstampClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitstampClient, error) {
	wsEndpoint := "wss://ws.bitstamp.net"

	bitstamp := BitstampClient{
		name:         "bitstamp",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	bitstamp.wsClient.SetMessageHandler(bitstamp.onMessage)

	log.Info("Created new datasource", "datasource", bitstamp.GetName())
	return &bitstamp, nil
}

func (b *BitstampClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()
	b.SetPing()

	return nil
}

func (b *BitstampClient) Reconnect() error {
	log.Info("Reconnecting...")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected", "datasource", b.GetName())
	err = b.SubscribeTickers()
	if err != nil {
		log.Error("Error subscribing to tickers", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}
func (b *BitstampClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BitstampClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"event":"trade"`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())
				return nil
			}

			fmt.Println(ticker)
			b.TickerTopic.Send(ticker)

		}
	}

	return nil
}

func (b *BitstampClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(strings.ReplaceAll(newTickerEvent.Channel, "live_trades_", ""))
	ts, err := strconv.ParseInt(newTickerEvent.Data.TimestampMicro, 10, 64)
	if err != nil {
		return nil, err
	}

	ticker := &model.Ticker{
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Symbol,
		LastPrice: newTickerEvent.Data.LastPrice,
		Source:    b.GetName(),
		Timestamp: time.UnixMicro(ts),
	}

	return ticker, nil
}

func (b *BitstampClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"event": "bts:subscribe",
			"data": map[string]interface{}{
				"channel": fmt.Sprintf("live_trades_%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			},
		}
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed ticker symbol", "datasource", b.GetName(), "symbols", v.Symbol)
	}

	log.Info("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *BitstampClient) GetName() string {
	return b.name
}
func (b *BitstampClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`{"event":"bts:heartbeat"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
