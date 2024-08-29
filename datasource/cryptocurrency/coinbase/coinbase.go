package coinbase

import (
	"fmt"
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

type CoinbaseClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger
}

func NewCoinbaseClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CoinbaseClient, error) {
	wsEndpoint := "wss://ws-feed.exchange.coinbase.com"

	coinbase := CoinbaseClient{
		name:        "coinbase",
		log:         slog.Default().With(slog.String("datasource", "coinbase")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	coinbase.wsClient.SetMessageHandler(coinbase.onMessage)
	coinbase.wsClient.SetOnConnect(coinbase.onConnect)

	coinbase.wsClient.SetLogger(coinbase.log)
	coinbase.log.Debug("Created new datasource")
	return &coinbase, nil
}

func (b *CoinbaseClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setLastTickerWatcher()

	return nil
}

func (b *CoinbaseClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (b *CoinbaseClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *CoinbaseClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"type":"subscriptions"`) {
			b.parseSubscriptions(message.Message)
			return
		}

		if strings.Contains(msg, `"type":"ticker"`) {
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

func (b *CoinbaseClient) parseTicker(message []byte) (*model.Ticker, error) {
	var tickerMessage CoinbaseTicker
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(tickerMessage.ProductId)
	ts, err := time.Parse(time.RFC3339, tickerMessage.Timestamp)
	if err != nil {
		return nil, err
	}

	ticker, err := model.NewTicker(tickerMessage.LastPrice,
		symbol,
		b.GetName(),
		ts)

	return ticker, err
}

func (b *CoinbaseClient) parseSubscriptions(message []byte) {
	var subscrSuccessMessage CoinbaseSubscriptionSuccessMessage
	err := sonic.Unmarshal(message, &subscrSuccessMessage)
	if err != nil {
		b.log.Error(err.Error())
		return
	}

	b.log.Debug("Subscribed ticker symbols",
		"symbols", len(subscrSuccessMessage.Channels[0].ProductIds))
}

func (b *CoinbaseClient) SubscribeTickers() error {
	s := []string{}
	for _, v := range b.SymbolList {
		s = append(s, fmt.Sprintf("%s-%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
	}
	subMessage := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": s,
		"channels":    []string{"ticker"},
	}

	b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	return nil
}

func (b *CoinbaseClient) GetName() string {
	return b.name
}

func (b *CoinbaseClient) setLastTickerWatcher() {
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
				b.wsClient.Reconnect()
			}
		}
	}()
}
