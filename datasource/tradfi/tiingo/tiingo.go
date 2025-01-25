package tiingo

import (
	"fmt"
	"strconv"
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

type TiingoClient struct {
	name           string
	W              *sync.WaitGroup
	TickerTopic    *tickertopic.TickerTopic
	wsClient       internal.WebSocketClient
	wsEndpoint     string
	SymbolList     []model.Symbol
	apiToken       string
	thresholdLevel int

	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewTiingoFxClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*TiingoClient, error) {
	wsEndpoint := "wss://api.tiingo.com/fx"

	tiingo := TiingoClient{
		name:           "tiingo_fx",
		log:            slog.Default().With(slog.String("datasource", "tiingo_fx")),
		W:              w,
		TickerTopic:    tickerTopic,
		wsClient:       *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:     wsEndpoint,
		SymbolList:     symbolList.Forex,
		pingInterval:   20,
		apiToken:       options["api_token"].(string),
		thresholdLevel: 5,
	}
	tiingo.wsClient.SetMessageHandler(tiingo.onMessage)
	tiingo.wsClient.SetOnConnect(tiingo.onConnect)

	tiingo.log.Info("Created new tiingo datasource")
	return &tiingo, nil
}

func NewTiingoIexClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*TiingoClient, error) {
	wsEndpoint := "wss://api.tiingo.com/iex"

	tiingo := TiingoClient{
		name:           "tiingo_iex",
		log:            slog.Default().With(slog.String("datasource", "tiingo_iex")),
		W:              w,
		TickerTopic:    tickerTopic,
		wsClient:       *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:     wsEndpoint,
		SymbolList:     symbolList.Forex,
		pingInterval:   20,
		apiToken:       options["api_token"].(string),
		thresholdLevel: 5,
	}
	tiingo.wsClient.SetMessageHandler(tiingo.onMessage)
	tiingo.wsClient.SetOnConnect(tiingo.onConnect)

	tiingo.log.Info("Created new tiingo datasource")
	return &tiingo, nil
}

func (b *TiingoClient) Connect() error {
	b.W.Add(1)
	b.log.Info("Connecting...")

	b.wsClient.Start()

	b.setPing()

	return nil
}

func (b *TiingoClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.setLastTickerWatcher()

	return nil
}

func (b *TiingoClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *TiingoClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"A"`) {
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

func (b *TiingoClient) parseTicker(message []byte) (*model.Ticker, error) {

	var newTickerEvent WsFxEvent
	err := sonic.Unmarshal(message, &newTickerEvent)
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
		Base:  symbol.Base,
		Quote: symbol.Quote,

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
			Base:  v.Base,
			Quote: v.Quote})
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
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *TiingoClient) GetName() string {
	return b.name
}

func (b *TiingoClient) setLastTickerWatcher() {
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

func (b *TiingoClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
		}
	}()
}
