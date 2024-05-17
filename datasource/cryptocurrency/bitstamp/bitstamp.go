package bitstamp

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BitstampClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewBitstampClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitstampClient, error) {
	wsEndpoint := "wss://ws.bitstamp.net"

	bitstamp := BitstampClient{
		name:         "bitstamp",
		log:          slog.Default().With(slog.String("datasource", "bitstamp")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	bitstamp.wsClient.SetMessageHandler(bitstamp.onMessage)

	bitstamp.wsClient.SetLogger(bitstamp.log)
	bitstamp.log.Debug("Created new datasource")
	return &bitstamp, nil
}

func (b *BitstampClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *BitstampClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()

	return nil
}
func (b *BitstampClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *BitstampClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"event":"trade"`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)
		}
	}
}

func (b *BitstampClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(strings.ReplaceAll(newTickerEvent.Channel, "live_trades_", ""))
	ts, err := strconv.ParseInt(newTickerEvent.Data.TimestampMicro, 10, 64)
	if err != nil {
		return nil, err
	}

	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		b.GetName(),
		time.UnixMicro(ts))

	return ticker, err
}

func (b *BitstampClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"event": "bts:subscribe",
			"data": map[string]interface{}{
				"channel": fmt.Sprintf("live_trades_%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			},
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *BitstampClient) GetName() string {
	return b.name
}

func (b *BitstampClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	b.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(b.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				b.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				b.lastTimestamp = time.Now()
				b.Reconnect()
				return
			}
		}
	}()
}
func (b *BitstampClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"event":"bts:heartbeat"}`)})
		}
	}()
}
