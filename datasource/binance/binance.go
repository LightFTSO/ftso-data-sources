package binance

import (
	"encoding/json"
	"fmt"
	"io"
	log "log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BinanceClient struct {
	name        string
	W           *sync.WaitGroup
	TradeTopic  *broadcast.Broadcaster
	TickerChan  *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	wsMessageChan chan internal.WsMessage
}

func NewBinanceClient(options interface{}, symbolList symbols.AllSymbols, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BinanceClient, error) {
	log.Info("Created new datasource", "datasource", "binance")
	wsEndpoint := "wss://stream.binance.com:9443/stream?streams="

	binance := BinanceClient{
		name:          "binance",
		W:             w,
		TradeTopic:    tradeTopic,
		TickerChan:    tickerTopic,
		wsClient:      *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:    wsEndpoint,
		apiEndpoint:   "https://api.binance.com",
		SymbolList:    symbolList.Crypto,
		wsMessageChan: make(chan internal.WsMessage),
	}
	binance.wsClient.SetMessageHandler(binance.onMessage)

	return &binance, nil
}

func (b *BinanceClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to binance datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *BinanceClient) Reconnect() error {
	log.Info("Reconnecting to binance datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected to binance datasource")
	err = b.SubscribeTrades()
	if err != nil {
		log.Error("Error subscribing to trades", "datasource", b.GetName())
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
		log.Error("Error reading websocket data, reconnecting in 5 seconds",
			"datasource", b.GetName(), "error", message.Err)
		time.Sleep(5 * time.Second)
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {

		if strings.Contains(string(message.Message), "@trade") {
			trade, err := b.parseTrade(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())

			}
			b.TradeTopic.Send(trade)
		}
	}

	return nil
}

func (b *BinanceClient) parseTrade(message []byte) (*model.Trade, error) {
	var newTradeEvent WsCombinedTradeEvent
	err := json.Unmarshal(message, &newTradeEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return &model.Trade{}, err
	}

	price, err := strconv.ParseFloat(newTradeEvent.Data.Price, 32)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseFloat(newTradeEvent.Data.Quantity, 32)
	if err != nil {
		return nil, err
	}
	symbol := model.ParseSymbol(newTradeEvent.Data.Symbol)
	newTrade := model.Trade{
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Symbol,
		Price:     price,
		Size:      size,
		Source:    b.GetName(),
		Timestamp: time.UnixMilli(newTradeEvent.Data.Time),
	}

	return &newTrade, nil
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
	    "btcusdt@trade",
	    "etcusdt@trade"
	  ],
	  "id": 1
	}

*
*/
func (b *BinanceClient) SubscribeTrades() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		log.Error("Error obtaining available symbols. Closing binance datasource %s", "error", err.Error())
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
		s = append(s, fmt.Sprintf("%s%s@trade", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
	}
	subMessage := map[string]interface{}{
		"method": "SUBSCRIBE",
		"id":     rand.Uint64(),
		"params": s,
	}

	b.wsClient.SendMessageJSON(subMessage)

	log.Info("Subscribed trade symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *BinanceClient) SubscribeTickers() error {
	return nil
}

func (b *BinanceClient) GetName() string {
	return b.name
}
