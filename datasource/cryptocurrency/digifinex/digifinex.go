package digifinex

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
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

type DigifinexClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval     time.Duration
	availableMarkets model.SymbolList

	isRunning bool
}

func NewDigifinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*DigifinexClient, error) {
	wsEndpoint := "wss://openapi.digifinex.com/ws/v1/"

	digifinex := DigifinexClient{
		name:         "digifinex",
		log:          slog.Default().With(slog.String("datasource", "digifinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://openapi.digifinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 15 * time.Second,
	}
	digifinex.symbolChunks = digifinex.SymbolList.ChunkSymbols(30)
	for _, v := range digifinex.symbolChunks {
		fmt.Println(len(v))
	}
	digifinex.log.Debug("Created new datasource")
	return &digifinex, nil
}

func (d *DigifinexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			time.Sleep(500 * time.Millisecond)
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

func (d *DigifinexClient) Close() error {
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

func (d *DigifinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *DigifinexClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		/*if strings.Contains(msg, `"table":"spot/ticker"`) {
		tickers, err := d.parseTicker(message.Message)
		if err != nil {
			d.log.Error("Error parsing ticker",
				"error", err.Error())
			return err
		}

		for _, v := range tickers {
			d.TickerTopic.Send(v)
		}
		}*/

		// decompress
		decompressedData, err := internal.DecompressZlib(message.Message)
		if err != nil {
			d.log.Error("Error decompressing message", "error", err.Error())
			return
		}
		data := string(decompressedData)
		if strings.Contains(data, "ticker.update") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			tickers, err := d.parseTicker([]byte(data))
			if err != nil {
				d.log.Error("Error parsing ticker", "error", err.Error())
				return
			}
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			for _, t := range tickers {
				d.TickerTopic.Send(t)
			}
		}
	}
}

func (d *DigifinexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Params {
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

	return tickers, nil
}

func (b *DigifinexClient) getAvailableSymbols() (model.SymbolList, error) {
	if b.availableMarkets != nil {
		return b.availableMarkets, nil
	}

	reqUrl := b.apiEndpoint + "/v3/markets"

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

	var exchangeInfo = new(MarketInfo)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	availableMarkets := model.SymbolList{}
	for _, v := range exchangeInfo.Data {
		availableMarkets = append(availableMarkets, model.ParseSymbol(v.Market))
	}

	b.availableMarkets = availableMarkets
	return availableMarkets, nil
}

func (d *DigifinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing digifinex datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []string{}
	for _, v1 := range symbols {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(string(v2.Base))) &&
				strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(string(v2.Quote))) {

				subscribedSymbols = append(subscribedSymbols, fmt.Sprintf("%s_%s", strings.ToUpper(v2.Base), strings.ToUpper(v2.Quote)))
			}
		}
	}

	// batch subscriptions in packets of 20
	chunksize := 20
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "ticker.subscribe",
			"id":     fmt.Sprint(rand.Uint32() % 9999),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, v)
		}
		subMessage["params"] = s
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *DigifinexClient) GetName() string {
	return d.name
}

func (d *DigifinexClient) setLastTickerWatcher() {
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

func (d *DigifinexClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			msg := map[string]interface{}{
				"ping":   fmt.Sprint(rand.Uint32() % 999999),
				"method": "ping",
				"params": []string{},
			}
			for _, wsClient := range d.wsClients {
				if err := wsClient.SendMessageJSON(websocket.TextMessage, msg); err != nil {
					d.log.Warn("Failed to send ping", "error", err)
				}
			}

		}
	}()
}
