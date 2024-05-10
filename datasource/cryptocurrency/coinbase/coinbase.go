package coinbase

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	log "log/slog"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type CoinbaseClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol
}

func NewCoinbaseClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CoinbaseClient, error) {
	wsEndpoint := "wss://ws-feed.pro.coinbase.com"

	coinbase := CoinbaseClient{
		name:        "coinbase",
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	coinbase.wsClient.SetMessageHandler(coinbase.onMessage)

	log.Debug("Created new datasource", "datasource", coinbase.GetName())
	return &coinbase, nil
}

func (b *CoinbaseClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *CoinbaseClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())

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

func (b *CoinbaseClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *CoinbaseClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"type":"subscriptions"`) {
			return b.parseSubscriptions(message.Message)
		}

		if strings.Contains(msg, `"type":"ticker"`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"ticker", ticker, "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *CoinbaseClient) parseTicker(message []byte) (*model.Ticker, error) {
	var tickerMessage CoinbaseTicker
	err := json.Unmarshal(message, &tickerMessage)
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

func (b *CoinbaseClient) parseSubscriptions(message []byte) error {
	var subscrSuccessMessage CoinbaseSubscriptionSuccessMessage
	err := json.Unmarshal(message, &subscrSuccessMessage)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return err
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(),
		"symbols", len(subscrSuccessMessage.Channels[0].ProductIds))

	return nil
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

	b.wsClient.SendMessageJSON(subMessage)

	return nil
}

func (b *CoinbaseClient) GetName() string {
	return b.name
}
