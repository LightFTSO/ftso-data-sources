package gateio

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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

type GateIoClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	isRunning bool
}

func NewGateIoClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*GateIoClient, error) {
	wsEndpoint := "wss://api.gateio.ws/ws/v4/"

	gateio := GateIoClient{
		name:         "gateio",
		log:          slog.Default().With(slog.String("datasource", "gateio")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.gateio.ws/api/v4",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	gateio.wsClient.SetMessageHandler(gateio.onMessage)
	gateio.wsClient.SetOnConnect(gateio.onConnect)

	gateio.wsClient.SetLogger(gateio.log)
	gateio.log.Debug("Created new datasource")
	return &gateio, nil
}

func (d *GateIoClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *GateIoClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (d *GateIoClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *GateIoClient) IsRunning() bool {
	return d.isRunning
}

func (d *GateIoClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "spot.tickers") && strings.Contains(string(message.Message), "\"event\":\"update\"") {
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

func (d *GateIoClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Result.CurrencyPair)
	ticker, err := model.NewTicker(newTickerEvent.Result.Last,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.TimeMs))

	return ticker, err
}

func (d *GateIoClient) getAvailableSymbols() (*[]GateIoInstrument, error) {
	reqUrl := d.apiEndpoint + "/spot/currency_pairs"

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

	var availableSymbols = new([]GateIoInstrument)
	err = sonic.Unmarshal(data, availableSymbols)
	if err != nil {
		return nil, err
	}
	return availableSymbols, nil

}

func (d *GateIoClient) SubscribeTickers() error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing gateio datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range d.SymbolList {
		for _, v2 := range *availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}

	// batch subscriptions in packets of 10
	chunksize := 10
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"time":    time.Now().UnixMilli(),
			"channel": "spot.tickers",
			"event":   "subscribe",
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["payload"] = s
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *GateIoClient) GetName() string {
	return d.name
}

func (d *GateIoClient) setLastTickerWatcher() {
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

func (d *GateIoClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"method":"server.ping"}`)})
		}
	}()
}
