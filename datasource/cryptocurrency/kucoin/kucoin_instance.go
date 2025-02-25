package kucoin

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type kucoinInstanceClient struct {
	name               string
	TickerTopic        *tickertopic.TickerTopic
	wsClient           internal.WebSocketClient
	wsEndpoint         string
	SymbolList         []model.Symbol
	availableSymbols   []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	pingInterval       time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
	log                *slog.Logger
	instanceToken      string
	instanceId         string
	pingTicker         *time.Ticker
	lastTickerTsTicker *time.Ticker
	closed             bool
	clientClosedChan   *broadcast.Broadcaster
}

func newKucoinInstanceClient(instanceServer InstanceServer, availableSymbols []model.Symbol, symbolList []model.Symbol, tickerTopic *tickertopic.TickerTopic, ctx context.Context, cancel context.CancelFunc) *kucoinInstanceClient {
	instanceId := rand.Uint64()
	wsEndpoint := fmt.Sprintf("%s?token=%s&connectId=%x", instanceServer.Endpoint, instanceServer.Token, instanceId)

	kucoinInstance := kucoinInstanceClient{
		name:             "kucoin",
		log:              slog.Default().With(slog.String("datasource", "kucoin"), slog.String("instanceId", fmt.Sprintf("%d", instanceId))),
		TickerTopic:      tickerTopic,
		wsClient:         *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:       wsEndpoint,
		instanceToken:    instanceServer.Token,
		instanceId:       fmt.Sprintf("%x", instanceId),
		SymbolList:       symbolList,
		availableSymbols: availableSymbols,
		pingInterval:     time.Duration(instanceServer.PingIntervalMs) * time.Millisecond,
		ctx:              ctx,
		cancel:           cancel,
		closed:           false,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	kucoinInstance.wsClient.SetMessageHandler(kucoinInstance.onMessage)
	kucoinInstance.wsClient.SetOnDisconnect(kucoinInstance.onDisconnect)
	kucoinInstance.wsClient.SetLogger(kucoinInstance.log)

	return &kucoinInstance
}

func (d *kucoinInstanceClient) connect() error {
	d.wsClient.Start()
	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *kucoinInstanceClient) onDisconnect() error {
	d.wsClient.ExplicitClose()
	d.closed = true
	d.clientClosedChan.Send(true)
	d.log.Debug("closing instance client")
	d.cancel()
	return nil
}

func (d *kucoinInstanceClient) onWelcomeMessage() error {
	err := d.subscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (d *kucoinInstanceClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, "welcome") && strings.Contains(msg, d.instanceId) {
			d.onWelcomeMessage()
		}

		if strings.Contains(msg, "/market/ticker:") {
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

func (d *kucoinInstanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(strings.ReplaceAll(newTickerEvent.Topic, "/market/ticker:", ""))
	ticker, err := model.NewTicker(newTickerEvent.Data.Price,
		symbol,
		d.getName(),
		time.UnixMilli(newTickerEvent.Data.Time))
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, nil
}

func (d *kucoinInstanceClient) subscribeTickers() error {
	subscribedSymbols := []model.Symbol{}
	for _, v1 := range d.SymbolList {
		for _, v2 := range d.availableSymbols {
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
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *kucoinInstanceClient) getName() string {
	return d.name
}

func (d *kucoinInstanceClient) setLastTickerWatcher() {
	d.lastTickerTsTicker = time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer d.lastTickerTsTicker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("last ticker received watcher goroutine exiting")
				return
			case <-d.lastTickerTsTicker.C:
				if d.closed {
					return
				}

				now := time.Now()
				d.lastTimestampMutex.Lock()
				diff := now.Sub(d.lastTimestamp)
				d.lastTimestampMutex.Unlock()

				if diff > timeout {
					// no tickers received in a while, close this kucoin instance
					d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
					d.onDisconnect()
					return
				}
			}
		}
	}()
}

func (d *kucoinInstanceClient) setPing() {
	d.pingTicker = time.NewTicker(d.pingInterval)
	go func() {
		defer d.pingTicker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("ping sender goroutine exiting")
				return
			case <-d.pingTicker.C:
				if d.closed {
					return
				}
				d.log.Debug("Sending ping message")
				d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(fmt.Sprintf(`{"id":"%d","type":"ping"}`, time.Now().UnixMicro()))})
			}
		}
	}()
}
