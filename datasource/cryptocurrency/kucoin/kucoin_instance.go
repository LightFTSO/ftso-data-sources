package kucoin

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	log "log/slog"

	"github.com/goccy/go-json"
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

	pingIntervalMs int
	ctx            context.Context
	cancel         context.CancelFunc

	instanceToken string
	instanceId    string
}

func newKucoinInstanceClient(instanceServer InstanceServer, availableSymbols []model.Symbol, symbolList []model.Symbol, tickerTopic *broadcast.Broadcaster, ctx context.Context, cancel context.CancelFunc) *kucoinInstanceClient {
	instanceId := rand.Uint64()
	wsEndpoint := fmt.Sprintf("%s?token=%s&connectId=%x", instanceServer.Endpoint, instanceServer.Token, instanceId)

	kucoinInstance := kucoinInstanceClient{
		name:             "kucoin",
		TickerTopic:      tickerTopic,
		wsClient:         *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:       wsEndpoint,
		instanceToken:    instanceServer.Token,
		instanceId:       fmt.Sprintf("%x", instanceId),
		SymbolList:       symbolList,
		availableSymbols: availableSymbols,
		pingIntervalMs:   instanceServer.PingIntervalMs,
		ctx:              ctx,
		cancel:           cancel,
	}

	log.Debug("Created new datasource", "datasource", kucoinInstance.getName())
	return &kucoinInstance
}

func (b *kucoinInstanceClient) connect() error {
	log.Info("Connecting...", "datasource", b.getName())

	b.wsClient.SetMessageHandler(b.onMessage)
	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	go b.wsClient.Listen()
	b.setPing()

	return nil
}

func (b *kucoinInstanceClient) close() error {
	b.wsClient.Close()

	return nil
}

func (b *kucoinInstanceClient) onWelcomeMessage() error {
	err := b.subscribeTickers()
	if err != nil {
		log.Error("Error subscribing to tickers", "datasource", b.getName())
		return err
	}

	return nil
}

func (b *kucoinInstanceClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.getName(), "error", message.Err)
		b.cancel()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, "welcome") && strings.Contains(msg, b.instanceId) {
			b.onWelcomeMessage()
		}

		if strings.Contains(msg, "/market/ticker:") {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.getName(),
					"ticker", ticker, "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *kucoinInstanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.getName())
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
		b.wsClient.SendMessageJSON(subMessage)

	}

	log.Debug("Subscribed ticker symbols", "datasource", b.getName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *kucoinInstanceClient) getName() string {
	return b.name
}

func (b *kucoinInstanceClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingIntervalMs) * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.Connection.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"id":"%d","type":"ping"}`, time.Now().UnixMicro()))); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.getName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
