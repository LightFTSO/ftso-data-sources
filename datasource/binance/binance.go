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
	internal "roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
)

type BinanceWebsocketClient struct {
	Name        string                   `json:"name"`
	W           *sync.WaitGroup          `json:"-"`
	TradeChan   *chan model.Trade        `json:"-"`
	wsClient    internal.WebsocketClient `json:"-"`
	wsEndpoint  string                   `json:"-"`
	apiEndpoint string                   `json:"-"`
	SymbolList  []model.Symbol           `json:"symbolList"`

	wsMessageChan chan internal.WsMessage
}

func NewBinanceWebsocketClient(symbolList []model.Symbol, tradeChan *chan model.Trade, w *sync.WaitGroup) *BinanceWebsocketClient {
	log.Info("Created new datasource", "datasource", "binance")
	wsEndpoint := "wss://stream.binance.com:9443/stream?streams="

	binance := BinanceWebsocketClient{
		Name:          "binance",
		W:             w,
		TradeChan:     tradeChan,
		wsClient:      *internal.NewWebsocketClient(wsEndpoint, true),
		wsEndpoint:    wsEndpoint,
		apiEndpoint:   "https://api.binance.com",
		SymbolList:    symbolList,
		wsMessageChan: make(chan internal.WsMessage),
	}

	return &binance
}

func (b *BinanceWebsocketClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to binance datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen(b.wsMessageChan)
	go b.Listen()

	return nil
}

func (b *BinanceWebsocketClient) Reconnect() error {
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
	go b.wsClient.Listen(b.wsMessageChan)
	return nil
}

func (b *BinanceWebsocketClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *BinanceWebsocketClient) Listen() {
	for message := range b.wsMessageChan {
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
					continue
				}
				*b.TradeChan <- *trade
			}
		}
	}
}

func (b *BinanceWebsocketClient) parseTrade(message []byte) (*model.Trade, error) {
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
		Base:   symbol.Base,
		Quote:  symbol.Quote,
		Symbol: symbol.Symbol,
		Price:  price,
		Size:   size,
		//Side:      newTradeEvent.Data.IsBuyerMaker,
		Source:    b.GetName(),
		Timestamp: time.UnixMilli(newTradeEvent.Data.Time),
	}

	return &newTrade, nil
}

func (b *BinanceWebsocketClient) getAvailableSymbols() ([]BinanceSymbol, error) {
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
func (b *BinanceWebsocketClient) SubscribeTrades() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		return fmt.Errorf("Error obtaining available symbols. Closing binance datasource %s", err.Error())

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

	subMessage := make(map[string]interface{})
	subMessage["method"] = "SUBSCRIBE"
	subMessage["id"] = rand.Uint64()
	s := []string{}
	for _, v := range subscribedSymbols {
		s = append(s, fmt.Sprintf("%s%s@trade", strings.ToLower(v.Base), strings.ToLower(v.Quote)))
	}
	subMessage["params"] = s
	b.wsClient.SendMessageJSON(subMessage)

	log.Info("Subscribed trade symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func keepAlive(c *websocket.Conn, timeout time.Duration) {
	ticker := time.NewTicker(timeout)

	lastResponse := time.Now()
	c.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		defer ticker.Stop()
		for {
			deadline := time.Now().Add(10 * time.Second)
			err := c.WriteControl(websocket.PingMessage, []byte{}, deadline)
			if err != nil {
				return
			}
			<-ticker.C
			if time.Since(lastResponse) > timeout {
				c.Close()
				return
			}
		}
	}()
}

func (b *BinanceWebsocketClient) GetName() string {
	return b.Name
}
