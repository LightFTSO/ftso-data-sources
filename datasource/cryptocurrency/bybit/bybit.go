package bybit

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type BybitClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *tickertopic.TickerTopic
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewBybitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BybitClient, error) {
	wsEndpoint := "wss://stream.bybit.com/v5/public/spot"

	bybit := BybitClient{
		name:         "bybit",
		log:          slog.Default().With(slog.String("datasource", "bybit")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bybit.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bybit.wsClient.SetMessageHandler(bybit.onMessage)
	bybit.wsClient.SetOnConnect(bybit.onConnect)

	bybit.wsClient.SetLogger(bybit.log)
	bybit.log.Debug("Created new datasource")
	return &bybit, nil
}

func (b *BybitClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *BybitClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	b.setPing()

	return nil
}
func (b *BybitClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *BybitClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "tickers.") {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}

			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)
		}
	}
}

func (b *BybitClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		b.GetName(),
		time.UnixMilli(newTickerEvent.Time))

	return ticker, err
}

func (b *BybitClient) getAvailableSymbols() ([]BybitSymbol, error) {
	reqUrl := b.apiEndpoint + "/v5/market/instruments-info?category=spot"

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

	type instrumentInfoResponse struct {
		InstrumentsInfo *InstrumentInfoResponse `json:"result"`
	}
	var exchangeInfo = new(instrumentInfoResponse)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}
	return exchangeInfo.InstrumentsInfo.List, nil

}

func (b *BybitClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing bybit datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.BaseCoin)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.QuoteCoin)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.BaseCoin,
					Quote: v2.QuoteCoin})
			}
		}
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"op":     "subscribe",
			"req_id": strconv.FormatUint(rand.Uint64(), 36),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("tickers.%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["args"] = s
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *BybitClient) GetName() string {
	return b.name
}

func (b *BybitClient) setLastTickerWatcher() {
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
				b.wsClient.Reconnect()
			}
		}
	}()
}

func (b *BybitClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"op":"ping"}`)})
		}
	}()
}
