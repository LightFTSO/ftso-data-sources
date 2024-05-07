package hitbtc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type HitbtcClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewHitbtcClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HitbtcClient, error) {
	wsEndpoint := "wss://api.hitbtc.com/api/3/ws/public"

	hitbtc := HitbtcClient{
		name:         "hitbtc",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	hitbtc.wsClient.SetMessageHandler(hitbtc.onMessage)

	log.Info("Created new datasource", "datasource", hitbtc.GetName())
	return &hitbtc, nil
}

func (b *HitbtcClient) Connect() error {
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

func (b *HitbtcClient) Reconnect() error {
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
func (b *HitbtcClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *HitbtcClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "ticker/price/1s") && strings.Contains(string(message.Message), "data") {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())
				return nil
			}

			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}
		}
	}

	return nil
}

func (b *HitbtcClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
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
		ticker := model.Ticker{
			Base:      symbol.Base,
			Quote:     symbol.Quote,
			Symbol:    symbol.Symbol,
			LastPrice: tickData.LastPrice,
			Source:    b.GetName(),
			Timestamp: time.UnixMilli(tickData.Timestamp),
		}
		tickers = append(tickers, &ticker)
	}

	return tickers, nil
}

func (b *HitbtcClient) SubscribeTickers() error {
	// batch subscriptions in packets of 5
	chunksize := 10
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
		//fmt.Println(subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *HitbtcClient) GetName() string {
	return b.name
}