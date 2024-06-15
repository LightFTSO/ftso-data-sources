package cryptocom

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type CryptoComClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	subscriptionId atomic.Uint64
}

func NewCryptoComClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CryptoComClient, error) {
	wsEndpoint := "wss://stream.crypto.com/v2/market"

	cryptocom := CryptoComClient{
		name:         "cryptocom",
		log:          slog.Default().With(slog.String("datasource", "cryptocom")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.crypto.com/v2",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	cryptocom.wsClient.SetMessageHandler(cryptocom.onMessage)
	cryptocom.wsClient.SetOnConnect(cryptocom.onConnect)

	cryptocom.wsClient.SetLogger(cryptocom.log)
	cryptocom.log.Debug("Created new datasource")
	return &cryptocom, nil
}

func (b *CryptoComClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	return nil
}

func (b *CryptoComClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	b.setLastTickerWatcher()

	return nil
}
func (b *CryptoComClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *CryptoComClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, "public/heartbeat") {
			b.pong(message.Message)
			return
		}

		if strings.Contains(msg, "\"channel\":\"ticker\"") && strings.Contains(msg, "\"subscription\":\"ticker.") {
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

func (b *CryptoComClient) parseTicker(message []byte) ([]*model.Ticker, error) {

	var tickerMessage WsTickerMessage
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	symbol := model.ParseSymbol(tickerMessage.Result.IntrumentName)
	tickers := []*model.Ticker{}
	for _, v := range tickerMessage.Result.Data {
		// some messages come with null data
		if v.LastPrice == "" {
			continue
		}

		newTicker, err := model.NewTicker(v.LastPrice,
			symbol,
			b.GetName(),
			time.UnixMilli(v.Timestamp))
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *CryptoComClient) SubscribeTickers() error {
	// batch subscriptions in packets of 5
	chunksize := 10
	for i := 0; i < len(b.SymbolList); i += chunksize {
		subMessage := map[string]interface{}{
			"id":     b.subscriptionId.Add(1),
			"method": "subscribe",
			"nonce":  time.Now().UnixMicro(),
			"params": map[string]interface{}{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(b.SymbolList) {
				continue
			}
			v := b.SymbolList[i+j]
			s = append(s, fmt.Sprintf("ticker.%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"channels": s,
		}

		// sleep a bit to avoid rate limits
		time.Sleep(20 * time.Millisecond)
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *CryptoComClient) GetName() string {
	return b.name
}

func (b *CryptoComClient) setLastTickerWatcher() {
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

func (b *CryptoComClient) pong(pingMessage []byte) {
	b.log.Debug("Sending pong message")
	var ping PublicHeartbeat
	err := sonic.Unmarshal(pingMessage, &ping)
	if err != nil {
		b.log.Error(err.Error())
		return
	}

	pong := ping
	pong.Method = "public/respond-heartbeat"

	if err := b.wsClient.SendMessageJSON(websocket.TextMessage, pong); err != nil {
		b.log.Warn("Failed to send ping", "error", err)
		return
	}
}
