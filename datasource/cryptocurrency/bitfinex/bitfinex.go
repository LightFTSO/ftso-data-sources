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

	subscriptionId atomic.Uint64
	pingInterval   time.Duration

	availableSymbols []model.Symbol
	apiSymbolMap     [][]string
	channelSymbolMap sync.Map

	isRunning bool
}

func NewBitfinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitfinexClient, error) {
	wsEndpoint := "wss://api-pub.bitfinex.com/ws/2"

	bitfinex := BitfinexClient{
		name:         "bitfinex",
		log:          slog.Default().With(slog.String("datasource", "bitfinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClients:    []*internal.WebSocketClient{},
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api-pub.bitfinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35 * time.Second,
	}

	bitfinex.fetchApiSymbolMap()
	//bitfinex.channelSymbolMap = make(ChannelSymbolMap)
	bitfinex.symbolChunks = bitfinex.SymbolList.ChunkSymbols(50)
	bitfinex.log.Debug("Created new datasource")
	return &bitfinex, nil
}

func (d *BitfinexClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	_, err := d.getAvailableSymbols()
	if err != nil {
		d.log.Error("error obtaining available symbols. Closing bitfinex datasource", "error", err.Error())
		d.W.Done()
		return err
	}

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
	d.W.Done()
	d.isRunning = false
	d.log.Info("Bitfinex closing")
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

	if strings.Contains(data, `subscribed`) && strings.Contains(data, `trade`) {
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

	trade, err := d.parseTicker(message.Message)
	if err != nil {
		d.log.Error("Error parsing trade",
			"error", err.Error(), "data", data)
		return
	}
	if trade == nil {
		return
	}
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	d.TickerTopic.Send(trade)
}

func (d *BitfinexClient) parseSubscribeMessage(msg []byte) error {
	var subscriptionMessage SubscribeMessage
	err := sonic.Unmarshal(msg, &subscriptionMessage)
	if err != nil {
		return err
	}

	if subscriptionMessage.Event == "subscribed" {
		d.channelSymbolMap.Store(subscriptionMessage.ChannelId, d.mapApiSymbolToNormalSymbol(strings.Replace(subscriptionMessage.Pair, ":", "", 1)))
	}

	return nil
}

func (d *BitfinexClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTradeEvent TickerEvent
	err := sonic.Unmarshal(message, &newTradeEvent)
	if err != nil {
		return nil, err
	}

	if len(newTradeEvent) != 3 {
		return nil, nil
	}

	if x, ok := newTradeEvent[1].(string); x != "te" || !ok {
		return nil, nil
	}
	channelId, ok := newTradeEvent[0].(float64)
	if !ok {
		return nil, nil
	}
	s, ok := d.channelSymbolMap.Load(int(channelId))
	if !ok {
		return nil, nil
	}
	s, ok = s.(string)
	if !ok {
		return nil, nil
	}

	symbol := model.ParseSymbol(s.(string))
	tradeData := newTradeEvent[2].([]interface{})

	if len(tradeData) != 4 {
		return nil, errors.New("unknown entity received")
	}

	price, ok := tradeData[3].(float64)
	if !ok {
		d.log.Error("Error parsing trade",
			"price", tradeData[3], "error", "unable to parse price value")
		return nil, nil
	}

	ts, ok := tradeData[1].(float64)
	if !ok {
		d.log.Error("Error parsing trade",
			"timestamp_value", tradeData[1], "error", "unable to parse timestamps value")
		return nil, nil
	}
	newTicker, err := model.NewTicker(fmt.Sprintf("%f", price),
		symbol,
		d.GetName(),
		time.UnixMilli(int64(ts)))
	if err != nil {
		d.log.Error("Error parsing trade",
			"trade", newTicker, "error", err.Error())
	}

	return newTicker, nil
}

func (d *BitfinexClient) getAvailableSymbols() (model.SymbolList, error) {
	d.log.Debug("Fetching available markets")
	reqUrl := d.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

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
	d.availableSymbols = availableMarkets
	return availableMarkets, nil
}

func (d *BitfinexClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	subscribedSymbols := model.SymbolList{}
	for _, v1 := range symbols {
		for _, v2 := range d.availableSymbols {
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
			"channel": "trades",
			"symbol":  fmt.Sprintf("t%s%s", base, d.mapNormalSymbolToApiSymbol(strings.ToUpper(v.Quote))),
		}

		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed trade symbols", "symbols", len(subscribedSymbols))
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

				d.log.Warn(fmt.Sprintf("No trades received in %s", diff))

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

	symbolMap := [][]string{{"UST", "USDT"}, {"TSD", "TUSD"}}

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
