package coinex

import (
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
	wsClient       internal.WebsocketClient
	wsEndpoint     string
	apiEndpoint    string
	SymbolList     []model.Symbol
	lastTimestamp  time.Time
	log            *slog.Logger
	subscriptionId atomic.Uint64
	pingInterval   int
}

func NewCoinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*CoinexClient, error) {
	wsEndpoint := "wss://socket.coinex.com/v2/spot"

	coinex := CoinexClient{
		name:         "coinex",
		log:          slog.Default().With(slog.String("datasource", "coinex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.coinex.com/v2",
		SymbolList:   symbolList.Crypto,
		pingInterval: 35,
	}
	coinex.wsClient.SetMessageHandler(coinex.onMessage)

	coinex.wsClient.SetLogger(coinex.log)
	coinex.log.Debug("Created new datasource")
	return &coinex, nil
}

func (b *CoinexClient) Connect() error {
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

func (b *CoinexClient) Reconnect() error {
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
func (b *CoinexClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *CoinexClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}
	decompressedData, err := internal.DecompressGzip(message.Message)
	if err != nil {
		b.log.Error("Error decompressing message", "error", err.Error())
		return
	}
	data := string(decompressedData)
	if strings.Contains(data, `"deals.update"`) {
		tickers, err := b.parseTicker(decompressedData)
		if err != nil {
			b.log.Error("Error parsing ticker",
				"error", err.Error())
			return
		}
		b.lastTimestamp = time.Now()

		for _, v := range tickers {
			b.TickerTopic.Send(v)
		}
	}

}

func (b *CoinexClient) comparePrices(s *model.Ticker) string { return s.LastPrice }

func (b *CoinexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	symbol := model.ParseSymbol(newTickerEvent.Data.Market)
	for _, t := range newTickerEvent.Data.DealList {
		newTicker, err := model.NewTicker(t.Price,
			symbol,
			b.GetName(),
			time.UnixMilli(t.Timestamp))
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	//compareSymbols := func(s *model.Ticker) string { return s.Symbol }
	if helpers.AreAllFieldsEqual(tickers, b.comparePrices) {
		return []*model.Ticker{tickers[0]}, nil
	}
	return tickers, nil
}

func (b *CoinexClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := b.apiEndpoint + "/spot/ticker"

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

func (b *CoinexClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing coinex datasource", "error", err.Error())
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

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "deals.subscribe",
			"params": map[string]interface{}{},
			"id":     b.subscriptionId.Add(1),
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
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(100 * time.Millisecond)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *CoinexClient) GetName() string {
	return b.name
}

func (b *CoinexClient) setLastTickerWatcher() {
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

func (b *CoinexClient) SetPing() {
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
