package lbank

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type LbankClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc

	subscriptionId atomic.Uint64
}

func NewLbankClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*LbankClient, error) {
	wsEndpoint := "wss://www.lbkex.net/ws/V2/"

	lbank := LbankClient{
		name:         "lbank",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	lbank.wsClient.SetMessageHandler(lbank.onMessage)

	log.Debug("Created new datasource", "datasource", lbank.GetName())
	return &lbank, nil
}

func (b *LbankClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()
	b.SetPing()

	return nil
}

func (b *LbankClient) Reconnect() error {
	log.Info("Reconnecting...")

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
func (b *LbankClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *LbankClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	msg := string(message.Message)

	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, `"type":"tick"`) {
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

func (b *LbankClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Pair)
	ts, err := time.Parse("2006-01-02T15:04:05.999", newTickerEvent.Timestamp)
	if err != nil {
		return nil, err
	}

	ticker, err := model.NewTicker(fmt.Sprintf("%.6f", newTickerEvent.Ticker.LastPrice),
		symbol,
		b.GetName(),
		ts)

	return ticker, err
}

func (b *LbankClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"action":    "subscribe",
			"subscribe": "tick",
			"pair":      fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed ticker symbol", "datasource", b.GetName(), "symbols", v.GetSymbol())
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *LbankClient) GetName() string {
	return b.name
}
func (b *LbankClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				id := b.subscriptionId.Add(1)
				msg := map[string]interface{}{
					"ping":   fmt.Sprintf("%d", id),
					"action": "ping",
				}
				if err := b.wsClient.Connection.WriteJSON(msg); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
