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
	name           string
	W              *sync.WaitGroup
	TickerTopic    *broadcast.Broadcaster
	wsClient       internal.WebsocketClient
	wsEndpoint     string
	apiEndpoint    string
	SymbolList     []model.Symbol
	lastTimestamp  time.Time
	log            *slog.Logger
	subscriptionId atomic.Uint64
	pingInterval   int

	apiSymbolMap     [][2]string
	channelSymbolMap ChannelSymbolMap
}

func NewBitfinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitfinexClient, error) {
	wsEndpoint := "wss://api-pub.bitfinex.com/ws/2"

	bitfinex := BitfinexClient{
		name:         "bitfinex",
		log:          slog.Default().With(slog.String("datasource", "bitfinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api-pub.bitfinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35,
	}
	bitfinex.wsClient.SetMessageHandler(bitfinex.onMessage)

	bitfinex.wsClient.SetLogger(bitfinex.log)
	bitfinex.fetchApiSymbolMap()
	bitfinex.channelSymbolMap = make(ChannelSymbolMap)
	bitfinex.log.Debug("Created new datasource")
	return &bitfinex, nil
}

func (b *BitfinexClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *BitfinexClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	b.SetPing()

	return nil
}
func (b *BitfinexClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *BitfinexClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}
	data := string(message.Message)
	if strings.Contains(data, `info`) {
		return
	}

	if strings.Contains(data, `hb`) { // heartbeat
		return
	}

	if strings.Contains(data, `subscribed`) && strings.Contains(data, `ticker`) {
		err := b.parseSubscribeMessage(message.Message)
		if err != nil {
			b.log.Error("Error parsing subscription message", "err", err)
		}
		return
	}

	if strings.Contains(data, "error") {
		b.log.Error("Error message from Bitfinex", "message", data)
		return
	}

	ticker, err := b.parseTicker(message.Message)
	if err != nil {
		b.log.Error("Error parsing ticker",
			"error", err.Error(), "data", data)
		return
	}
	b.lastTimestamp = time.Now()

	b.TickerTopic.Send(ticker)
}

func (b *BitfinexClient) parseSubscribeMessage(msg []byte) error {
	var subscriptionMessage SubscribeMessage
	err := sonic.Unmarshal(msg, &subscriptionMessage)
	if err != nil {
		return err
	}

	if subscriptionMessage.Event == "subscribed" {
		b.channelSymbolMap[subscriptionMessage.ChannelId] = b.mapApiSymbolToNormalSymbol(strings.Replace(subscriptionMessage.Pair, ":", "", 1))
	}

	return nil
}

func (b *BitfinexClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent TickerEvent
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return nil, err
	}
	if len(newTickerEvent) == 2 {
		symbol := model.ParseSymbol(b.channelSymbolMap[int(newTickerEvent[0].(float64))])
		t := newTickerEvent[1].([]interface{})
		tickerData := t
		price := tickerData[6].(float64)
		newTicker, err := model.NewTicker(fmt.Sprintf("%f", price),
			symbol,
			b.GetName(),
			time.Now())
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
		}

		fmt.Println(newTicker)
		return newTicker, nil
	}

	return nil, errors.New("")
}

func (b *BitfinexClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := b.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

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

	availableMarkets := []model.Symbol{}
	for _, v := range bitfinexMarkets[0] {
		s := b.mapApiSymbolToNormalSymbol(v)
		availableMarkets = append(availableMarkets, model.ParseSymbol(s))
	}

	return availableMarkets, nil
}

func (b *BitfinexClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing bitfinex datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}

	for _, v := range subscribedSymbols {
		base := b.mapNormalSymbolToApiSymbol(strings.ToUpper(v.Base))
		if len(base) > 3 {
			base = fmt.Sprintf("%s:", base)
		}
		subMessage := map[string]interface{}{
			"event":   "subscribe",
			"channel": "ticker",
			"symbol":  fmt.Sprintf("t%s%s", base, b.mapNormalSymbolToApiSymbol(strings.ToUpper(v.Quote))),
		}

		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *BitfinexClient) GetName() string {
	return b.name
}

func (b *BitfinexClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	b.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(b.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				b.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				b.lastTimestamp = time.Now()
				b.Reconnect()
				return
			}
		}
	}()
}

func (b *BitfinexClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			ping := map[string]interface{}{
				"method": "server.ping",
				"params": map[string]interface{}{},
				"id":     b.subscriptionId.Add(1),
			}
			b.wsClient.SendMessageJSON(websocket.PingMessage, ping)
		}
	}()
}

func (b *BitfinexClient) fetchApiSymbolMap() error {
	/*reqUrl := b.apiEndpoint + "/v2/conf/pub:list:pair:exchange"

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

	b.apiSymbolMap = symbolMap[0]
	return nil*/
	symbolMap := [][2]string{{"UST", "USDT"}, {"TSD", "TUSD"}}

	b.apiSymbolMap = symbolMap

	return nil
}

func (b *BitfinexClient) mapApiSymbolToNormalSymbol(apiSymbol string) string {
	for _, m := range b.apiSymbolMap {
		apiSymbol = strings.ReplaceAll(apiSymbol, m[0], m[1])
	}
	return apiSymbol
}

func (b *BitfinexClient) mapNormalSymbolToApiSymbol(apiSymbol string) string {
	for _, m := range b.apiSymbolMap {
		if strings.Contains(apiSymbol, m[1]) {
			return strings.Replace(apiSymbol, m[1], m[0], 1)
		}
	}
	return apiSymbol
}
