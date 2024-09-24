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
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval     int
	availableMarkets []model.Symbol

	isRunning bool
}

func NewDigifinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*DigifinexClient, error) {
	wsEndpoint := "wss://openapi.digifinex.com/ws/v1/"

	digifinex := DigifinexClient{
		name:         "digifinex",
		log:          slog.Default().With(slog.String("datasource", "digifinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://openapi.digifinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	digifinex.wsClient.SetMessageHandler(digifinex.onMessage)
	digifinex.wsClient.SetOnConnect(digifinex.onConnect)

	digifinex.wsClient.SetLogger(digifinex.log)
	digifinex.log.Debug("Created new datasource")
	return &digifinex, nil
}

func (d *DigifinexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *DigifinexClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (d *DigifinexClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
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
			d.lastTimestamp = time.Now()
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

func (b *DigifinexClient) getAvailableSymbols() ([]model.Symbol, error) {
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

	availableMarkets := []model.Symbol{}
	for _, v := range exchangeInfo.Data {
		availableMarkets = append(availableMarkets, model.ParseSymbol(v.Market))
	}

	b.availableMarkets = availableMarkets
	return availableMarkets, nil
}

func (d *DigifinexClient) SubscribeTickers() error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing digifinex datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []string{}
	for _, v1 := range d.SymbolList {
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
			"id":     fmt.Sprint(rand.Uint32() % 999999),
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
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *DigifinexClient) GetName() string {
	return d.name
}

func (d *DigifinexClient) setLastTickerWatcher() {
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

func (d *DigifinexClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			msg := map[string]interface{}{
				"ping":   fmt.Sprint(rand.Uint32() % 999999),
				"method": "ping",
				"params": []string{},
			}
			if err := d.wsClient.SendMessageJSON(websocket.TextMessage, msg); err != nil {
				d.log.Warn("Failed to send ping", "error", err)
			}
		}
	}()
}
