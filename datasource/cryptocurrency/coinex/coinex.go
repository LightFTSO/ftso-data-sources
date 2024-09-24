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
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type CoinexClient struct {
	name           string
	W              *sync.WaitGroup
	TickerTopic    *broadcast.Broadcaster
	wsClient       internal.WebSocketClient
	wsEndpoint     string
	apiEndpoint    string
	SymbolList     []model.Symbol
	lastTimestamp  time.Time
	log            *slog.Logger
	subscriptionId atomic.Uint64
	pingInterval   int
	isRunning      bool
}

func NewCoinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CoinexClient, error) {
	wsEndpoint := "wss://socket.coinex.com/v2/spot"

	coinex := CoinexClient{
		name:         "coinex",
		log:          slog.Default().With(slog.String("datasource", "coinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.coinex.com/v2",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35,
	}
	coinex.wsClient.SetMessageHandler(coinex.onMessage)
	coinex.wsClient.SetOnConnect(coinex.onConnect)

	coinex.wsClient.SetLogger(coinex.log)
	coinex.log.Debug("Created new datasource")
	return &coinex, nil
}

func (d *CoinexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *CoinexClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	d.setPing()

	return nil
}
func (d *CoinexClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
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
		d.lastTimestamp = time.Now()

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

func (d *CoinexClient) getAvailableSymbols() ([]model.Symbol, error) {
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

	availableMarkets := []model.Symbol{}
	for _, v := range exchangeInfo.Data {
		availableMarkets = append(availableMarkets, model.ParseSymbol(v.Market))
	}

	return availableMarkets, nil
}

func (d *CoinexClient) SubscribeTickers() error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing coinex datasource", "error", err.Error())
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
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

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

func (d *CoinexClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			ping := map[string]interface{}{
				"method": "server.ping",
				"params": map[string]interface{}{},
				"id":     d.subscriptionId.Add(1),
			}
			d.wsClient.SendMessageJSON(websocket.PingMessage, ping)
		}
	}()
}
