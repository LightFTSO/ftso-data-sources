package pionex

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
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type PionexClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
	wsClient           *internal.WebSocketClient
	wsEndpoint         string
	SymbolList         []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger
	apiEndpoint        string

	pingInterval time.Duration

	isRunning bool
}

func NewPionexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*PionexClient, error) {
	wsEndpoint := "wss://ws.pionex.com/wsPub"

	pionex := PionexClient{
		name:         "pionex",
		log:          slog.Default().With(slog.String("datasource", "pionex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15 * time.Second,
		apiEndpoint:  "https://api.pionex.com/api/v1",
	}
	pionex.wsClient.SetMessageHandler(pionex.onMessage)
	pionex.wsClient.SetOnConnect(pionex.onConnect)

	pionex.wsClient.SetLogger(pionex.log)
	pionex.log.Debug("Created new datasource")
	return &pionex, nil
}

func (d *PionexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setLastTickerWatcher()

	return nil
}

func (d *PionexClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (d *PionexClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *PionexClient) IsRunning() bool {
	return d.isRunning
}

func (d *PionexClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		msg := string(message.Message)
		if strings.Contains(msg, `"event":"subscribe"`) {
			return
		}

		if strings.Contains(msg, `"topic":"TRADE"`) && !strings.Contains(msg, "SUBSCRIBED") {
			tickers, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"error", err.Error())
				return
			}
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			for _, v := range tickers {
				d.TickerTopic.Send(v)
			}
		}

		if strings.Contains(msg, "PING") {
			pong := strings.ReplaceAll(msg, "PING", "PONG")
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(pong)})
			d.log.Debug("Pong received")
			return
		}
	}
}

func (d *PionexClient) comparePrices(s *model.Ticker) string { return s.LastPrice }

func (d *PionexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.LastPrice,
			symbol,
			d.GetName(),
			time.UnixMilli(t.Timestamp))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	//compareSymbols := func(s *model.Ticker) string { return s.Symbol }
	if helpers.AreAllFieldsEqual(tickers, d.comparePrices) {
		return []*model.Ticker{tickers[0]}, nil
	}
	return tickers, nil
}

func (d *PionexClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := d.apiEndpoint + "/common/symbols"

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

	var symbolsData = new(SymbolsResponse)
	err = sonic.Unmarshal(data, symbolsData)
	if err != nil {
		return nil, err
	}

	symbols := []model.Symbol{}
	for _, s := range symbolsData.Data.Symbols {
		symbols = append(symbols, model.Symbol{
			Base:  s.BaseCurrency,
			Quote: s.QuoteCurrency,
		})
	}
	return symbols, nil

}

func (d *PionexClient) SubscribeTickers() error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing bybit datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range d.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}
	for _, v := range subscribedSymbols {
		subMessage := map[string]interface{}{
			"op":     "SUBSCRIBE",
			"topic":  "TRADE",
			"symbol": fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
		time.Sleep(100 * time.Millisecond)
	}

	d.log.Debug("Subscribed ticker symbols")

	return nil
}

func (d *PionexClient) GetName() string {
	return d.name
}

func (d *PionexClient) setLastTickerWatcher() {
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

				d.wsClient.Reconnect()
			}
		}
	}()
}
