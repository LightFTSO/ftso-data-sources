package coinbase

import (
	"errors"
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
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
	wsClient           *internal.WebSocketClient
	wsEndpoint         string
	SymbolList         []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger
	isRunning          bool
}

func NewCoinbaseClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CoinbaseClient, error) {
	wsEndpoint := "wss://ws-feed.exchange.coinbase.com"

	coinbase := CoinbaseClient{
		name:        "coinbase",
		log:         slog.Default().With(slog.String("datasource", "coinbase")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	coinbase.wsClient.SetMessageHandler(coinbase.onMessage)
	coinbase.wsClient.SetOnConnect(coinbase.onConnect)

	coinbase.wsClient.SetLogger(coinbase.log)
	coinbase.log.Debug("Created new datasource")
	return &coinbase, nil
}

func (d *CoinbaseClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setLastTickerWatcher()

	return nil
}

func (d *CoinbaseClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (d *CoinbaseClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *CoinbaseClient) IsRunning() bool {
	return d.isRunning
}

func (d *CoinbaseClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"type":"subscriptions"`) {
			d.parseSubscriptions(message.Message)
			return
		}

		if strings.Contains(msg, `"type":"ticker"`) {
			ticker, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			d.TickerTopic.Send(ticker)
		}
	}
}

func (d *CoinbaseClient) parseTicker(message []byte) (*model.Ticker, error) {
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
		d.GetName(),
		ts)
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *CoinbaseClient) parseSubscriptions(message []byte) {
	var subscrSuccessMessage CoinbaseSubscriptionSuccessMessage
	err := sonic.Unmarshal(message, &subscrSuccessMessage)
	if err != nil {
		d.log.Error(err.Error())
		return
	}

	d.log.Debug("Subscribed ticker symbols",
		"symbols", len(subscrSuccessMessage.Channels[0].ProductIds))
}

func (d *CoinbaseClient) SubscribeTickers() error {
	s := []string{}
	for _, v := range d.SymbolList {
		s = append(s, fmt.Sprintf("%s-%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
	}
	subMessage := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": s,
		"channels":    []string{"ticker"},
	}

	d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	return nil
}

func (d *CoinbaseClient) GetName() string {
	return d.name
}

func (d *CoinbaseClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			d.lastTimestampMutex.Lock()
			diff := now.Sub(d.lastTimestamp)
			d.lastTimestampMutex.Unlock()

			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.lastTimestampMutex.Lock()
				d.lastTimestamp = time.Now()
				d.lastTimestampMutex.Unlock()

				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))

				d.wsClient.Reconnect()
			}
		}
	}()
}
