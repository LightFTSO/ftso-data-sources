package pionex

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type PionexClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *tickertopic.TickerTopic
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger
	apiEndpoint   string

	pingInterval int
}

func NewPionexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*PionexClient, error) {
	wsEndpoint := "wss://ws.pionex.com/wsPub"

	pionex := PionexClient{
		name:         "pionex",
		log:          slog.Default().With(slog.String("datasource", "pionex")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
		apiEndpoint:  "https://api.pionex.com/api/v1",
	}
	pionex.wsClient.SetMessageHandler(pionex.onMessage)
	pionex.wsClient.SetOnConnect(pionex.onConnect)

	pionex.wsClient.SetLogger(pionex.log)
	pionex.log.Debug("Created new datasource")
	return &pionex, nil
}

func (b *PionexClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setLastTickerWatcher()

	return nil
}

func (b *PionexClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (b *PionexClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *PionexClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		msg := string(message.Message)
		if strings.Contains(msg, `"event":"subscribe"`) {
			return
		}

		if strings.Contains(msg, `"topic":"TRADE"`) && !strings.Contains(msg, "SUBSCRIBED") {
			tickers, err := b.parseTicker(message.Message)
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

		if strings.Contains(msg, "PING") {
			pong := strings.ReplaceAll(msg, "PING", "PONG")
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(pong)})
			b.log.Debug("Pong received")
			return
		}
	}
}

func (b *PionexClient) comparePrices(s *model.Ticker) string { return s.LastPrice }

func (b *PionexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.LastPrice,
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

func (b *PionexClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := b.apiEndpoint + "/common/symbols"

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

	var symbolsData = new(SymbolsResponse)
	err = sonic.Unmarshal(data, symbolsData)
	if err != nil {
		return nil, err
	}

	symbols := []model.Symbol{}
	for _, s := range symbolsData.Data.Symbols {
		symbols = append(symbols, model.Symbol{
			Base:  s.BaseCurrency,
			Quote: s.QuoteCurrency,
		})
	}
	return symbols, nil

}

func (b *PionexClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing bybit datasource", "error", err.Error())
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
		subMessage := map[string]interface{}{
			"op":     "SUBSCRIBE",
			"topic":  "TRADE",
			"symbol": fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
		time.Sleep(100 * time.Millisecond)
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *PionexClient) GetName() string {
	return b.name
}

func (b *PionexClient) setLastTickerWatcher() {
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
