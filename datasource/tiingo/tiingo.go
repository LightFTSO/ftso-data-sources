package tiingo

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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

type TiingoClient struct {
	name           string
	W              *sync.WaitGroup
	TradeTopic     *broadcast.Broadcaster
	TickerTopic    *broadcast.Broadcaster
	wsClient       internal.WebsocketClient
	wsEndpoint     string
	SymbolList     []model.Symbol
	apiToken       string
	thresholdLevel int

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewTiingoClient(options map[string]interface{}, symbolList symbols.AllSymbols, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*TiingoClient, error) {
	log.Info("Created new tiingo datasource", "datasource", "tiingo")
	wsEndpoint := "wss://api.tiingo.com/fx"

	tiingo := TiingoClient{
		name:           "tiingo",
		W:              w,
		TradeTopic:     tradeTopic,
		TickerTopic:    tickerTopic,
		wsClient:       *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:     wsEndpoint,
		SymbolList:     symbolList.Forex,
		pingInterval:   20,
		apiToken:       options["api_token"].(string),
		thresholdLevel: 5,
	}
	tiingo.wsClient.SetMessageHandler(tiingo.onMessage)

	return &tiingo, nil
}

func (b *TiingoClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to tiingo datasource")

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	b.wsClient.SetKeepAlive(b.wsClient.Connection, 15*time.Second)

	go b.wsClient.Listen()

	return nil
}

func (b *TiingoClient) Reconnect() error {
	log.Info("Reconnecting to tiingo datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected to tiingo datasource")
	err = b.SubscribeTrades()
	if err != nil {
		log.Error("Error subscribing to trades", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}

func (b *TiingoClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *TiingoClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket data, reconnecting in 5 seconds",
			"datasource", b.GetName(), "error", message.Err)
		time.Sleep(1 * time.Second)
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		//fmt.Println(string(message.Message))
		if strings.Contains(string(message.Message), `"A"`) {
			trade, err := b.parseTrade(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())
				return nil

			}
			b.TradeTopic.Send(trade)
		}
	}

	return nil
}

func (b *TiingoClient) parseTrade(message []byte) (*model.Trade, error) {

	var newTradeEvent WsFxEvent
	err := json.Unmarshal(message, &newTradeEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return &model.Trade{}, err
	}

	tr := newTradeEvent.Data

	pair := tr[1]
	symbol := model.ParseSymbol(pair.(string))

	ts, err := time.Parse("2006-01-02T15:04:05.999999+00:00", tr[2].(string))
	if err != nil {
		return nil, err
	}
	ask := tr[4].(float64)
	bid := tr[5].(float64)
	price := (ask + bid) / 2

	trade := &model.Trade{
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Symbol,
		Price:     strconv.FormatFloat(price, 'f', 9, 64),
		Size:      "0",
		Source:    b.GetName(),
		Side:      "none",
		Timestamp: ts,
	}
	return trade, nil
}

func (b *TiingoClient) SubscribeTrades() error {
	subscribedSymbols := []model.Symbol{}
	for _, v := range b.SymbolList {
		subscribedSymbols = append(subscribedSymbols, model.Symbol{
			Base:   v.Base,
			Quote:  v.Quote,
			Symbol: fmt.Sprintf("%s/%s", v.Base, v.Quote)})
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"eventName":     "subscribe",
			"authorization": b.apiToken,
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
		}
		subMessage["eventData"] = map[string]interface{}{
			"thresholdLevel": b.thresholdLevel,
			"tickers":        s,
		}
		fmt.Println(subMessage)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed trade symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *TiingoClient) SubscribeTickers() error {
	return nil
}

func (b *TiingoClient) GetName() string {
	return b.name
}
