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

func NewTiingoFxClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*TiingoClient, error) {
	log.Info("Created new tiingo datasource", "datasource", "tiingo_fx")
	wsEndpoint := "wss://api.tiingo.com/fx"

	tiingo := TiingoClient{
		name:           "tiingo_fx",
		W:              w,
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

func NewTiingoIexClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*TiingoClient, error) {
	log.Info("Created new tiingo datasource", "datasource", "tiingo_iex")
	wsEndpoint := "wss://api.tiingo.com/iex"

	tiingo := TiingoClient{
		name:           "tiingo_iex",
		W:              w,
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
	log.Info("Connecting...", "datasource", b.GetName())

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
	log.Info("Reconnecting...", "datasource", b.GetName())

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

func (b *TiingoClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *TiingoClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		//fmt.Println(string(message.Message))
		if strings.Contains(string(message.Message), `"A"`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil

			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *TiingoClient) parseTicker(message []byte) (*model.Ticker, error) {

	var newTickerEvent WsFxEvent
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}

	tr := newTickerEvent.Data

	pair := tr[1]
	symbol := model.ParseSymbol(pair.(string))

	ts, err := time.Parse("2006-01-02T15:04:05.999999+00:00", tr[2].(string))
	if err != nil {
		return nil, err
	}
	ask := tr[4].(float64)
	bid := tr[5].(float64)
	price := (ask + bid) / 2

	ticker := &model.Ticker{
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Symbol,
		LastPrice: strconv.FormatFloat(price, 'f', 9, 64),
		Source:    b.GetName(),
		Timestamp: ts,
	}
	return ticker, nil
}

func (b *TiingoClient) SubscribeTickers() error {
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
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *TiingoClient) GetName() string {
	return b.name
}
