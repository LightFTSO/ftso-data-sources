package cryptocom

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

type CryptoComClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc

	subscriptionId atomic.Uint64
}

func NewCryptoComClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CryptoComClient, error) {
	wsEndpoint := "wss://stream.crypto.com/v2/market"

	cryptocom := CryptoComClient{
		name:         "cryptocom",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.crypto.com/v2",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	cryptocom.wsClient.SetMessageHandler(cryptocom.onMessage)

	log.Info("Created new datasource", "datasource", cryptocom.GetName())
	return &cryptocom, nil
}

func (b *CryptoComClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *CryptoComClient) Reconnect() error {
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
func (b *CryptoComClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *CryptoComClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, "public/heartbeat") {
			b.pong(message.Message)
			return nil
		}

		if strings.Contains(msg, "\"channel\":\"ticker\"") && strings.Contains(msg, "\"subscription\":\"ticker.") {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())
				return nil
			}

			for _, t := range tickers {
				b.TickerTopic.Send(t)
			}
		}
	}

	return nil
}

func (b *CryptoComClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var tickerMessage WsTickerMessage
	err := json.Unmarshal(message, &tickerMessage)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Ticker{}, err
	}

	symbol := model.ParseSymbol(tickerMessage.Result.IntrumentName)
	tickers := []*model.Ticker{}
	for _, v := range tickerMessage.Result.Data {
		t := model.Ticker{
			Base:      symbol.Base,
			Quote:     symbol.Quote,
			Symbol:    symbol.Symbol,
			LastPrice: v.LastPrice,
			Source:    b.GetName(),
			Timestamp: time.UnixMilli(v.Timestamp),
		}

		tickers = append(tickers, &t)
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
		//fmt.Println(subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(20 * time.Millisecond)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *CryptoComClient) GetName() string {
	return b.name
}

func (b *CryptoComClient) pong(pingMessage []byte) {
	var ping PublicHeartbeat
	err := json.Unmarshal(pingMessage, &ping)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return
	}

	pong := ping
	pong.Method = "public/respong-heartbeat"

	if err := b.wsClient.Connection.WriteJSON(pong); err != nil {
		log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
		return
	}
}
