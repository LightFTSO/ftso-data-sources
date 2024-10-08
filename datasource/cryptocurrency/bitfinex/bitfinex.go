package bitfinex

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
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BitfinexClient struct {
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
	subscriptionId     atomic.Uint64
	pingInterval       time.Duration

	apiSymbolMap     [][2]string
	channelSymbolMap ChannelSymbolMap

	isRunning bool
}

func NewBitfinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitfinexClient, error) {
	wsEndpoint := "wss://api-pud.bitfinex.com/ws/2"

	bitfinex := BitfinexClient{
		name:         "bitfinex",
		log:          slog.Default().With(slog.String("datasource", "bitfinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api-pud.bitfinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35 * time.Second,
	}

	bitfinex.fetchApiSymbolMap()
	bitfinex.channelSymbolMap = make(ChannelSymbolMap)
	bitfinex.symbolChunks = bitfinex.SymbolList.ChunkSymbols(30)
	bitfinex.log.Debug("Created new datasource")
	return &bitfinex, nil
}

func (d *BitfinexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, chunk)
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}
	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *BitfinexClient) Close() error {
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

func (d *BitfinexClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitfinexClient) onMessage(message internal.WsMessage) {
	data := string(message.Message)
	if strings.Contains(data, `info`) {
		return
	}

	if strings.Contains(data, `hb`) { // heartbeat
		return
	}

	if strings.Contains(data, `subscribed`) && strings.Contains(data, `ticker`) {
		err := d.parseSubscribeMessage(message.Message)
		if err != nil {
			d.log.Error("Error parsing subscription message", "err", err)
		}
		return
	}

	if strings.Contains(data, "error") {
		d.log.Error("Error message from Bitfinex", "message", data)
		return
	}

	ticker, err := d.parseTicker(message.Message)
	if err != nil {
		d.log.Error("Error parsing ticker",
			"error", err.Error(), "data", data)
		return
	}
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	d.TickerTopic.Send(ticker)
}

func (d *BitfinexClient) parseSubscribeMessage(msg []byte) error {
	var subscriptionMessage SubscribeMessage
	err := sonic.Unmarshal(msg, &subscriptionMessage)
	if err != nil {
		return err
	}

	if subscriptionMessage.Event == "subscribed" {
		d.channelSymbolMap[subscriptionMessage.ChannelId] = d.mapApiSymbolToNormalSymbol(strings.Replace(subscriptionMessage.Pair, ":", "", 1))
	}

	return nil
}

func (d *BitfinexClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent TickerEvent
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return nil, err
	}
	if len(newTickerEvent) == 2 {
		symbol := model.ParseSymbol(d.channelSymbolMap[int(newTickerEvent[0].(float64))])
		t := newTickerEvent[1].([]interface{})
		tickerData := t
		price := tickerData[6].(float64)
		newTicker, err := model.NewTicker(fmt.Sprintf("%f", price),
			symbol,
			d.GetName(),
			time.Now())
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
		}

		return newTicker, nil
	}

	return nil, errors.New("")
}

func (d *BitfinexClient) getAvailableSymbols() (model.SymbolList, error) {
	reqUrl := d.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

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

	var bitfinexMarkets [][]string
	err = sonic.Unmarshal(data, &bitfinexMarkets)
	if err != nil {
		return nil, err
	}

	availableMarkets := model.SymbolList{}
	for _, v := range bitfinexMarkets[0] {
		s := d.mapApiSymbolToNormalSymbol(v)
		availableMarkets = append(availableMarkets, model.ParseSymbol(s))
	}

	return availableMarkets, nil
}

func (d *BitfinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.W.Done()
		d.log.Error("error obtaining available symbols. Closing bitfinex datasource", "error", err.Error())
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

	for _, v := range subscribedSymbols {
		base := d.mapNormalSymbolToApiSymbol(strings.ToUpper(v.Base))
		if len(base) > 3 {
			base = fmt.Sprintf("%s:", base)
		}
		subMessage := map[string]interface{}{
			"event":   "subscribe",
			"channel": "ticker",
			"symbol":  fmt.Sprintf("t%s%s", base, d.mapNormalSymbolToApiSymbol(strings.ToUpper(v.Quote))),
		}

		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (d *BitfinexClient) GetName() string {
	return d.name
}

func (d *BitfinexClient) setLastTickerWatcher() {
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

func (d *BitfinexClient) setPing() {
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

func (d *BitfinexClient) fetchApiSymbolMap() error {
	/*reqUrl := d.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var symbolMap = [][][]string{{}}
	err = sonic.Unmarshal(data, symbolMap)
	if err != nil {
		return err
	}

	d.apiSymbolMap = symbolMap[0]
	return nil*/
	symbolMap := [][2]string{{"UST", "USDT"}, {"TSD", "TUSD"}}

	d.apiSymbolMap = symbolMap

	return nil
}

func (d *BitfinexClient) mapApiSymbolToNormalSymbol(apiSymbol string) string {
	for _, m := range d.apiSymbolMap {
		apiSymbol = strings.ReplaceAll(apiSymbol, m[0], m[1])
	}
	return apiSymbol
}

func (d *BitfinexClient) mapNormalSymbolToApiSymbol(apiSymbol string) string {
	for _, m := range d.apiSymbolMap {
		if strings.Contains(apiSymbol, m[1]) {
			return strings.Replace(apiSymbol, m[1], m[0], 1)
		}
	}
	return apiSymbol
}
