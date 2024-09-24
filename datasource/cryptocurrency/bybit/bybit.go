package bybit

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
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BybitClient struct {
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

func NewBybitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BybitClient, error) {
	wsEndpoint := "wss://stream.bybit.com/v5/public/spot"

	bybit := BybitClient{
		name:         "bybit",
		log:          slog.Default().With(slog.String("datasource", "bybit")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bybit.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bybit.wsClient.SetMessageHandler(bybit.onMessage)
	bybit.wsClient.SetOnConnect(bybit.onConnect)

	bybit.wsClient.SetLogger(bybit.log)
	bybit.log.Debug("Created new datasource")
	return &bybit, nil
}

func (d *BybitClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *BybitClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	d.setPing()

	return nil
}
func (d *BybitClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *BybitClient) IsRunning() bool {
	return d.isRunning
}

func (d *BybitClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "tickers.") {
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

func (d *BybitClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.Time))

	return ticker, err
}

func (d *BybitClient) getAvailableSymbols() ([]BybitSymbol, error) {
	reqUrl := d.apiEndpoint + "/v5/market/instruments-info?category=spot"

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

	type instrumentInfoResponse struct {
		InstrumentsInfo *InstrumentInfoResponse `json:"result"`
	}
	var exchangeInfo = new(instrumentInfoResponse)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}
	return exchangeInfo.InstrumentsInfo.List, nil

}

func (d *BybitClient) SubscribeTickers() error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing bybit datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range d.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.BaseCoin)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.QuoteCoin)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.BaseCoin,
					Quote: v2.QuoteCoin})
			}
		}
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"op":     "subscribe",
			"req_id": strconv.FormatUint(rand.Uint64(), 36),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("tickers.%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["args"] = s
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *BybitClient) GetName() string {
	return d.name
}

func (d *BybitClient) setLastTickerWatcher() {
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

func (d *BybitClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"op":"ping"}`)})
		}
	}()
}
