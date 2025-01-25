package kraken

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/go-multierror"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type KrakenClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *tickertopic.TickerTopic
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	isRunning bool
}

func NewKrakenClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*KrakenClient, error) {
	wsEndpoint := "wss://ws.kraken.com/v2"

	kraken := KrakenClient{
		name:         "kraken",
		log:          slog.Default().With(slog.String("datasource", "kraken")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.kraken.com/0",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20 * time.Second,
	}

	kraken.symbolChunks = kraken.SymbolList.ChunkSymbols(1024)
	kraken.log.Debug("Created new datasource")
	return &kraken, nil
}

func (d *KrakenClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

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

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *KrakenClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *KrakenClient) IsRunning() bool {
	return d.isRunning
}

func (d *KrakenClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "pong") {
			d.PongHandler(message.Message)
			return
		}
		if strings.Contains(string(message.Message), `"channel":"ticker"`) &&
			!strings.Contains(string(message.Message), `"method":"subscribe"`) {
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

func (d *KrakenClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent KrakenSnapshotUpdate
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}

	tickerData := newTickerEvent.Data[0]
	symbol := model.ParseSymbol(tickerData.Symbol)

	base := KrakenAsset(symbol.Base)

	ticker, err := model.NewTicker(strconv.FormatFloat(tickerData.Last, 'f', 9, 64),
		model.Symbol{Base: string(base.GetStdName()),
			Quote: symbol.Quote},
		d.GetName(),
		time.Now())
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *KrakenClient) getAvailableSymbols() ([]AssetPairInfo, error) {
	reqUrl := d.apiEndpoint + "/public/AssetPairs"

	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var exchangeInfo = new(ApiAssetPairResponse)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	if len(exchangeInfo.Error) > 0 {
		var apierrors error
		for _, apierr := range exchangeInfo.Error {
			multierror.Append(apierrors, errors.New(apierr))
		}
		return nil, apierrors
	}

	assetPairs := []AssetPairInfo{}

	for _, v := range exchangeInfo.Result {
		assetPairs = append(assetPairs, v)
	}

	return assetPairs, nil

}

func (d *KrakenClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.log.Error("error obtaining available symbols. Closing kraken datasource", "error", err.Error())
		d.W.Done()
		return err
	}

	subscribedSymbols := []string{}
	for _, v1 := range symbols {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(string(v2.Base.GetStdName()))) &&
				strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(string(v2.Quote.GetStdName()))) {

				subscribedSymbols = append(subscribedSymbols, (v2.WsName))
			}
		}
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "subscribe",
			"req_id": rand.Uint32(),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, v)
		}
		subMessage["params"] = map[string]interface{}{
			"channel":       "ticker",
			"event_trigger": "trades",
			"snapshot":      true,
			"symbol":        s,
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *KrakenClient) GetName() string {
	return d.name
}

func (d *KrakenClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
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
	}()
}

func (d *KrakenClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			for _, wsClient := range d.wsClients {
				wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(`{"event":"ping"}`)})
			}
		}
	}()
}

func (d *KrakenClient) PongHandler(msg []byte) {
	d.log.Debug("Pong received", "datasource", "kraken")
}
