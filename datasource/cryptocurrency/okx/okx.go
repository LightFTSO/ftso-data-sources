package okx

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

type OkxClient struct {
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

func NewOkxClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*OkxClient, error) {
	wsEndpoint := "wss://ws.okx.com:8443/ws/v5/public"

	okx := OkxClient{
		name:        "okx",
		log:         slog.Default().With(slog.String("datasource", "okx")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,

		pingInterval: 29,
	}
	okx.wsClient.SetMessageHandler(okx.onMessage)

	okx.wsClient.SetLogger(okx.log)
	okx.log.Debug("Created new datasource")
	return &okx, nil
}

func (b *OkxClient) Connect() error {
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

func (b *OkxClient) Reconnect() error {

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

func (b *OkxClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *OkxClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"channel":"index-tickers"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()

			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}

		}
	}
}

func (b *OkxClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var tickerMessage OkxTicker
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, v := range tickerMessage.Data {
		symbol := model.ParseSymbol(v.InstId)

		ts, err := strconv.ParseInt(v.Ts, 10, 64)
		if err != nil {
			return nil, err
		}

		newTicker, err := model.NewTicker(v.Idxpx,
			symbol,
			b.GetName(),
			time.UnixMilli(ts))
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *OkxClient) SubscribeTickers() error {
	s := []map[string]interface{}{}
	for _, v := range b.SymbolList {
		s = append(s, map[string]interface{}{
			"channel": "index-tickers",
			"instId": fmt.Sprintf("%s-%s",
				strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		})
	}
	subMessage := map[string]interface{}{
		"op":   "subscribe",
		"args": s,
	}

	b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	return nil
}

func (b *OkxClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`ping`)})
		}
	}()
}

func (b *OkxClient) GetName() string {
	return b.name
}

func (b *OkxClient) setLastTickerWatcher() {
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
