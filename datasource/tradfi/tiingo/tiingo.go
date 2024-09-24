package tiingo

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type TiingoClient struct {
	name           string
	W              *sync.WaitGroup
	TickerTopic    *broadcast.Broadcaster
	wsClient       internal.WebSocketClient
	wsEndpoint     string
	SymbolList     []model.Symbol
	apiToken       string
	thresholdLevel int

	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	isRunning bool
}

func NewTiingoFxClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*TiingoClient, error) {
	wsEndpoint := "wss://api.tiingo.com/fx"

	apiToken := ""
	if options["api_token"] == nil {
		apiToken = ""
	} else {
		apiToken = options["api_token"].(string)
	}

	tiingo := TiingoClient{
		name:           "tiingo_fx",
		log:            slog.Default().With(slog.String("datasource", "tiingo_fx")),
		W:              w,
		TickerTopic:    tickerTopic,
		wsClient:       *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:     wsEndpoint,
		SymbolList:     symbolList.Forex,
		pingInterval:   20,
		apiToken:       apiToken,
		thresholdLevel: 5,
	}
	tiingo.wsClient.SetMessageHandler(tiingo.onMessage)
	tiingo.wsClient.SetOnConnect(tiingo.onConnect)

	tiingo.log.Info("Created new tiingo datasource")
	return &tiingo, nil
}

func NewTiingoIexClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*TiingoClient, error) {
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

func (d *TiingoClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)
	d.log.Info("Connecting...")

	d.wsClient.Start()

	d.setPing()

	return nil
}

func (d *TiingoClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	d.setLastTickerWatcher()

	return nil
}

func (d *TiingoClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *TiingoClient) IsRunning() bool {
	return d.isRunning
}

func (d *TiingoClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"A"`) {
			ticker, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return

			}
			d.lastTimestamp = time.Now()
			d.TickerTopic.Send(ticker)
		}
	}
}

func (d *TiingoClient) parseTicker(message []byte) (*model.Ticker, error) {

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
		Source:    d.GetName(),
		Timestamp: ts,
	}
	return ticker, nil
}

func (d *TiingoClient) SubscribeTickers() error {
	subscribedSymbols := []model.Symbol{}
	for _, v := range d.SymbolList {
		subscribedSymbols = append(subscribedSymbols, model.Symbol{
			Base:  v.Base,
			Quote: v.Quote})
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"eventName":     "subscribe",
			"authorization": d.apiToken,
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
			"thresholdLevel": d.thresholdLevel,
			"tickers":        s,
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *TiingoClient) GetName() string {
	return d.name
}

func (d *TiingoClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(d.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				d.lastTimestamp = time.Now()
				d.wsClient.Reconnect()
			}
		}
	}()
}

func (d *TiingoClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
		}
	}()
}
