package kucoin

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
)

type kucoinInstanceClient struct {
	name             string
	TickerTopic      *broadcast.Broadcaster
	wsClient         internal.WebsocketClient
	wsEndpoint       string
	SymbolList       []model.Symbol
	availableSymbols []model.Symbol
	lastTimestamp    time.Time
	pingIntervalMs   int
	ctx              context.Context
	cancel           context.CancelFunc
	log              *slog.Logger
	instanceToken    string
	instanceId       string
}

func newKucoinInstanceClient(instanceServer InstanceServer, availableSymbols []model.Symbol, symbolList []model.Symbol, tickerTopic *broadcast.Broadcaster, ctx context.Context, cancel context.CancelFunc) *kucoinInstanceClient {
	instanceId := rand.Uint64()
	wsEndpoint := fmt.Sprintf("%s?token=%s&connectId=%x", instanceServer.Endpoint, instanceServer.Token, instanceId)

	kucoinInstance := kucoinInstanceClient{
		name:             "kucoin",
		log:              slog.Default().With(slog.String("datasource", "kucoin")),
		TickerTopic:      tickerTopic,
		wsClient:         *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:       wsEndpoint,
		instanceToken:    instanceServer.Token,
		instanceId:       fmt.Sprintf("%x", instanceId),
		SymbolList:       symbolList,
		availableSymbols: availableSymbols,
		pingIntervalMs:   instanceServer.PingIntervalMs,
		ctx:              ctx,
		cancel:           cancel,
	}
	kucoinInstance.wsClient.SetMessageHandler(kucoinInstance.onMessage)
	kucoinInstance.wsClient.SetLogger(kucoinInstance.log)
	kucoinInstance.log.Debug("Created new datasource")
	return &kucoinInstance
}

func (b *kucoinInstanceClient) connect() error {

	b.wsClient.Connect()
	err := b.subscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *kucoinInstanceClient) close() error {
	b.wsClient.Disconnect()
	b.cancel()

	return nil
}

func (b *kucoinInstanceClient) onWelcomeMessage() error {
	err := b.subscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (b *kucoinInstanceClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.close()
		return
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, "welcome") && strings.Contains(msg, b.instanceId) {
			b.onWelcomeMessage()
		}

		if strings.Contains(msg, "/market/ticker:") {
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

func (b *kucoinInstanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(strings.ReplaceAll(newTickerEvent.Topic, "/market/ticker:", ""))
	ticker, err := model.NewTicker(newTickerEvent.Data.Price,
		symbol,
		b.getName(),
		time.UnixMilli(newTickerEvent.Data.Time))

	return ticker, err
}

func (b *kucoinInstanceClient) subscribeTickers() error {
	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range b.availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}

	for _, symbol := range subscribedSymbols {
		subMessage := map[string]interface{}{
			"id":       rand.Uint32(),
			"type":     "subscribe",
			"topic":    "subscribe",
			"response": true,
		}

		subMessage["topic"] = fmt.Sprintf("/market/ticker:%s-%s", strings.ToUpper(symbol.Base), strings.ToUpper(symbol.Quote))
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *kucoinInstanceClient) getName() string {
	return b.name
}

func (b *kucoinInstanceClient) setLastTickerWatcher() {
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
				b.close()
			}
		}
	}()
}

func (b *kucoinInstanceClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingIntervalMs) * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(fmt.Sprintf(`{"id":"%d","type":"ping"}`, time.Now().UnixMicro()))})
		}
	}()
}
