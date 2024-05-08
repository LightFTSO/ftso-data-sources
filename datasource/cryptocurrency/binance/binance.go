package binance

import (
	"fmt"
	"io"
	log "log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BinanceClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol
}

func NewBinanceClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.com:443/stream?streams="

	binance := BinanceClient{
		name:        "binance",
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.binance.com",
		SymbolList:  symbolList.Crypto,
	}
	binance.wsClient.SetMessageHandler(binance.onMessage)

	log.Debug("Created new datasource", "datasource", binance.GetName())
	return &binance, nil
}

func (b *BinanceClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *BinanceClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected", "datasource", b.GetName())
	err = b.SubscribeTickers()
	if err != nil {
		log.Error("Error subscribing to tickers", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}

func (b *BinanceClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *BinanceClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {

		if strings.Contains(string(message.Message), "@ticker") {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *BinanceClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}
	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	newTicker := model.Ticker{
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Symbol,
		LastPrice: newTickerEvent.Data.LastPrice,
		Source:    b.GetName(),
		Timestamp: time.UnixMilli(newTickerEvent.Data.Time),
	}

	return &newTicker, nil
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
	err = json.Unmarshal(data, exchangeInfo)
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
		log.Error("Error obtaining available symbols. Closing binance datasource %s", "datasource", b.GetName(), "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.BaseAsset)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.QuoteAsset)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:   v2.BaseAsset,
					Quote:  v2.QuoteAsset,
					Symbol: fmt.Sprintf("%s/%s", v2.BaseAsset, v2.QuoteAsset)})
			}
		}
	}

	s := []string{}
	for _, v := range subscribedSymbols {
		s = append(s, fmt.Sprintf("%s%s@ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
	}
	subMessage := map[string]interface{}{
		"method": "SUBSCRIBE",
		"id":     rand.Uint64(),
		"params": s,
	}

	b.wsClient.SendMessageJSON(subMessage)

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *BinanceClient) GetName() string {
	return b.name
}
