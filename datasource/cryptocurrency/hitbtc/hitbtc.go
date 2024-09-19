package hitbtc

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

type HitbtcClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	isRunning bool
}

func NewHitbtcClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HitbtcClient, error) {
	wsEndpoint := "wss://api.hitbtc.com/api/3/ws/public"

	hitbtc := HitbtcClient{
		name:         "hitbtc",
		log:          slog.Default().With(slog.String("datasource", "hitbtc")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	hitbtc.wsClient.SetMessageHandler(hitbtc.onMessage)
	hitbtc.wsClient.SetOnConnect(hitbtc.onConnect)

	hitbtc.wsClient.SetLogger(hitbtc.log)
	hitbtc.log.Debug("Created new datasource")
	return &hitbtc, nil
}

func (b *HitbtcClient) Connect() error {
	b.isRunning = true
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *HitbtcClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (b *HitbtcClient) Close() error {
	b.wsClient.Close()
	b.isRunning = false
	b.W.Done()

	return nil
}

func (b *HitbtcClient) IsRunning() bool {
	return b.isRunning
}

func (b *HitbtcClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "ticker/price/1s") && strings.Contains(string(message.Message), "data") {
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

func (b *HitbtcClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	keys := make([]string, 0, len(newTickerEvent.Data))
	for k := range newTickerEvent.Data {
		keys = append(keys, k)
	}

	tickers := []*model.Ticker{}
	for _, key := range keys {
		tickData := newTickerEvent.Data[key]
		symbol := model.ParseSymbol(key)
		newTicker, err := model.NewTicker(tickData.LastPrice,
			symbol,
			b.GetName(),
			time.UnixMilli(tickData.Timestamp))
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *HitbtcClient) SubscribeTickers() error {
	// batch subscriptions in packets
	chunksize := len(b.SymbolList)
	for i := 0; i < len(b.SymbolList); i += chunksize {
		subMessage := map[string]interface{}{
			"ch":     "ticker/price/1s/batch",
			"method": "subscribe",
			"id":     time.Now().UnixMicro(),
			"params": map[string]interface{}{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(b.SymbolList) {
				continue
			}
			v := b.SymbolList[i+j]
			s = append(s, fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"symbols": s,
		}

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *HitbtcClient) GetName() string {
	return b.name
}

func (b *HitbtcClient) setLastTickerWatcher() {
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

func (b *HitbtcClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
		}
	}()
}
