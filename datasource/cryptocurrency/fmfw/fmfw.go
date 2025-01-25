package fmfw

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type FmfwClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *tickertopic.TickerTopic
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewFmfwClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*FmfwClient, error) {
	wsEndpoint := "wss://api.fmfw.io/api/3/ws/public"

	fmfw := FmfwClient{
		name:         "fmfw",
		log:          slog.Default().With(slog.String("datasource", "fmfw")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	fmfw.wsClient.SetMessageHandler(fmfw.onMessage)
	fmfw.wsClient.SetOnConnect(fmfw.onConnect)

	fmfw.wsClient.SetLogger(fmfw.log)
	fmfw.log.Debug("Created new datasource")
	return &fmfw, nil
}

func (b *FmfwClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *FmfwClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (b *FmfwClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *FmfwClient) onMessage(message internal.WsMessage) {
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

func (b *FmfwClient) parseTicker(message []byte) ([]*model.Ticker, error) {
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

func (b *FmfwClient) SubscribeTickers() error {
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

func (b *FmfwClient) GetName() string {
	return b.name
}

func (b *FmfwClient) setLastTickerWatcher() {
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

func (b *FmfwClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
		}
	}()
}
