package binance

import (
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
)

type BinanceClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger
}

func NewBinanceClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.com:443/stream?streams="

	binance := BinanceClient{
		name:        "binance",
		log:         slog.Default().With(slog.String("datasource", "binance")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.binance.com",
		SymbolList:  symbolList.Crypto,
	}
	binance.wsClient.SetMessageHandler(binance.onMessage)

	binance.wsClient.SetLogger(binance.log)
	binance.log.Debug("Created new datasource")
	return &binance, nil
}

func (b *BinanceClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.setLastTickerWatcher()

	return nil
}

func (b *BinanceClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (b *BinanceClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *BinanceClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	if message.Type == websocket.TextMessage {

		if strings.Contains(string(message.Message), "@ticker") {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)
			return
		}
	}
}

func (b *BinanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}
	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		b.GetName(),
		time.UnixMilli(newTickerEvent.Data.Time))

	return ticker, err
}

func (b *BinanceClient) getAvailableSymbols() ([]BinanceSymbol, error) {
	reqUrl := b.apiEndpoint + "/api/v3/exchangeInfo?permissions=SPOT"

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
func (b *BinanceClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("Error obtaining available symbols. Closing binance datasource %s", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
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

	b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *BinanceClient) GetName() string {
	return b.name
}

func (b *BinanceClient) setLastTickerWatcher() {
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
