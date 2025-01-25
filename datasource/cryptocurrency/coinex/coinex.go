package coinex

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type CoinexClient struct {
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
	subscriptionId     atomic.Uint64
	pingInterval       time.Duration
	isRunning          bool
}

func NewCoinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*CoinexClient, error) {
	wsEndpoint := "wss://socket.coinex.com/v2/spot"

	coinex := CoinexClient{
		name:         "coinex",
		log:          slog.Default().With(slog.String("datasource", "coinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.coinex.com/v2",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35 * time.Second,
	}
	coinex.symbolChunks = coinex.SymbolList.ChunkSymbols(2048)
	coinex.log.Debug("Created new datasource")
	return &coinex, nil
}

func (d *CoinexClient) Connect() error {
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

func (d *CoinexClient) Close() error {
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

func (d *CoinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *CoinexClient) onMessage(message internal.WsMessage) {
	decompressedData, err := internal.DecompressGzip(message.Message)
	if err != nil {
		d.log.Error("Error decompressing message", "error", err.Error())
		return
	}
	data := string(decompressedData)
	if strings.Contains(data, `"deals.update"`) {
		tickers, err := d.parseTicker(decompressedData)
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

}

func (d *CoinexClient) comparePrices(s *model.Ticker) string { return s.LastPrice }

func (d *CoinexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	symbol := model.ParseSymbol(newTickerEvent.Data.Market)
	for _, t := range newTickerEvent.Data.DealList {
		newTicker, err := model.NewTicker(t.Price,
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

func (d *CoinexClient) getAvailableSymbols() (model.SymbolList, error) {
	reqUrl := d.apiEndpoint + "/spot/ticker"

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

	var exchangeInfo = new(CoinexMarkets)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	availableMarkets := model.SymbolList{}
	for _, v := range exchangeInfo.Data {
		availableMarkets = append(availableMarkets, model.ParseSymbol(v.Market))
	}

	return availableMarkets, nil
}

func (d *CoinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.log.Error("error obtaining available symbols. Closing coinex datasource", "error", err.Error())
		d.W.Done()
		return err
	}

	subscribedSymbols := model.SymbolList{}
	for _, v1 := range symbols {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "deals.subscribe",
			"params": map[string]interface{}{},
			"id":     d.subscriptionId.Add(1),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"market_list": s,
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(100 * time.Millisecond)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *CoinexClient) GetName() string {
	return d.name
}

func (d *CoinexClient) setLastTickerWatcher() {
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

func (d *CoinexClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			ping := map[string]interface{}{
				"method": "server.ping",
				"params": map[string]interface{}{},
				"id":     d.subscriptionId.Add(1),
			}
			for _, wsClient := range d.wsClients {
				wsClient.SendMessageJSON(websocket.PingMessage, ping)
			}
		}
	}()
}
