package binance

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type BinanceClient struct {
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
	isRunning          bool
	clientClosedChan   *broadcast.Broadcaster
}

func NewBinanceClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.com:443/stream?streams="

	binance := BinanceClient{
		name:             "binance",
		log:              slog.Default().With(slog.String("datasource", "binance")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.binance.com",
		SymbolList:       symbolList.Crypto,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	binance.symbolChunks = binance.SymbolList.ChunkSymbols(1024)
	binance.log.Debug("Created new datasource")
	return &binance, nil
}

func (d *BinanceClient) Connect() error {
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
	d.setLastTickerWatcher()

	return nil
}

func (d *BinanceClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.W.Done()
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.log.Info("Binance closing")
	return nil
}

func (d *BinanceClient) IsRunning() bool {
	return d.isRunning
}

func (d *BinanceClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {

		if strings.Contains(string(message.Message), "@ticker") {
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
			return
		}
	}
}

func (d *BinanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}
	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTickerPriceString(newTickerEvent.Data.LastPrice,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.Data.Time))
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *BinanceClient) getAvailableSymbols() ([]BinanceSymbol, error) {
	reqUrl := d.apiEndpoint + "/api/v3/exchangeInfo?permissions=SPOT"

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

	var exchangeInfo = new(BinanceExchangeInfoResponse)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	return exchangeInfo.Symbols, nil

}

/*
*

	{
	  "method": "SUBSCRIBE",
	  "params": [
	    "btcusdt@ticker",
	    "etcusdt@ticker"
	  ],
	  "id": 1
	}

*
*/
func (d *BinanceClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.log.Error("Error obtaining available symbols. Closing binance datasource %s", "error", err.Error())
		d.W.Done()
		return err
	}

	subscribedSymbols := model.SymbolList{}
	for _, v1 := range symbols {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.BaseAsset)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.QuoteAsset)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.BaseAsset,
					Quote: v2.QuoteAsset})
			}
		}
	}

	s := []string{}
	for _, v := range subscribedSymbols {
		s = append(s, fmt.Sprintf("%s%s@ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
	}
	subMessage := map[string]interface{}{
		"method": "SUBSCRIBE",
		"id":     rand.Uint32() % 999999999,
		"params": s,
	}

	wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *BinanceClient) GetName() string {
	return d.name
}

func (d *BinanceClient) setLastTickerWatcher() {
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
