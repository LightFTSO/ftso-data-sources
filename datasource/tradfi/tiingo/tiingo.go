package tiingo

import (
	"errors"
	"fmt"
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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type TiingoClient struct {
	name           string
	W              *sync.WaitGroup
	TickerTopic    *tickertopic.TickerTopic
	wsClients      []*internal.WebSocketClient
	wsEndpoint     string
	SymbolList     model.SymbolList
	symbolChunks   []model.SymbolList
	apiToken       string
	thresholdLevel int

	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewTiingoFxClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*TiingoClient, error) {
	wsEndpoint := "wss://api.tiingo.com/fx"

	apiToken := ""
	if options["api_token"] == nil {
		apiToken = ""
	} else {
		apiToken = options["api_token"].(string)
	}

	tiingo := TiingoClient{
		name:             "tiingo_fx",
		log:              slog.Default().With(slog.String("datasource", "tiingo_fx")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		SymbolList:       symbolList.Forex,
		pingInterval:     20 * time.Second,
		apiToken:         apiToken,
		thresholdLevel:   5,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	tiingo.symbolChunks = tiingo.SymbolList.ChunkSymbols(1024)
	tiingo.log.Debug("Created new datasource")

	tiingo.log.Info("Created new tiingo datasource")
	return &tiingo, nil
}

func NewTiingoIexClient(options map[string]interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*TiingoClient, error) {
	wsEndpoint := "wss://api.tiingo.com/iex"

	tiingo := TiingoClient{
		name:             "tiingo_iex",
		log:              slog.Default().With(slog.String("datasource", "tiingo_iex")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		SymbolList:       symbolList.Forex,
		pingInterval:     20 * time.Second,
		apiToken:         options["api_token"].(string),
		thresholdLevel:   5,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	tiingo.symbolChunks = tiingo.SymbolList.ChunkSymbols(1024)
	tiingo.log.Debug("Created new datasource")

	tiingo.log.Info("Created new tiingo datasource")
	return &tiingo, nil
}

func (d *TiingoClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)
	d.log.Info("Connecting...")

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			err := d.SubscribeTickers(wsClient, chunk)
			if err != nil {
				d.log.Error("Error subscribing to tickers")
				return err
			}
			return err
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}
	d.setLastTickerWatcher()
	d.setPing()

	return nil
}

func (d *TiingoClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.clientClosedChan.Send(true)
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
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

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

		Price:     price,
		Source:    d.GetName(),
		Timestamp: ts,
	}
	return ticker, nil
}

func (d *TiingoClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	subscribedSymbols := model.SymbolList{}
	for _, v := range symbols {
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
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *TiingoClient) GetName() string {
	return d.name
}

func (d *TiingoClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("last ticker received watcher goroutine exiting")
				return
			case <-lastTickerIntervalTimer.C:
				now := time.Now()
				d.lastTimestampMutex.Lock()
				diff := now.Sub(d.lastTimestamp)
				d.lastTimestampMutex.Unlock()

				if diff > timeout {
					// no tickers received in a while, attempt to reconnect
					d.lastTimestampMutex.Lock()
					d.lastTimestamp = time.Now()
					d.lastTimestampMutex.Unlock()

					d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))

					for _, wsClient := range d.wsClients {
						wsClient.Reconnect()
					}
				}
			}
		}
	}()
}

func (d *TiingoClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("ping sender goroutine exiting")
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
				}
			}
		}
	}()
}
