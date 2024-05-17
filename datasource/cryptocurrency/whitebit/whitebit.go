package whitebit

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
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

type WhitebitClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	subscriptionId atomic.Uint64
}

func NewWhitebitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*WhitebitClient, error) {
	wsEndpoint := "wss://api.whitebit.com/ws"

	whitebit := WhitebitClient{
		name:         "whitebit",
		log:          slog.Default().With(slog.String("datasource", "whitebit")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://whitebit.com/api/v4/",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	whitebit.wsClient.SetMessageHandler(whitebit.onMessage)

	whitebit.wsClient.SetLogger(whitebit.log)
	whitebit.log.Debug("Created new datasource")
	return &whitebit, nil
}

func (b *WhitebitClient) Connect() error {
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

func (b *WhitebitClient) Reconnect() error {
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
func (b *WhitebitClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *WhitebitClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	msg := string(message.Message)
	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, "lastprice_update") {
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

func (b *WhitebitClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	if len(newTickerEvent.Params) != 2 {
		err = fmt.Errorf("received params have unknown data: %+v", newTickerEvent)
		return &model.Ticker{}, err
	}

	_, err = strconv.ParseFloat(newTickerEvent.Params[1], 64)
	if err != nil {
		return nil, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Params[0])
	ticker, err := model.NewTicker(newTickerEvent.Params[1],
		symbol,
		b.GetName(),
		time.Now())

	return ticker, err
}

func (b *WhitebitClient) getAvailableSymbols() ([]WhitebitMarketPair, error) {
	reqUrl := b.apiEndpoint + "public/markets"

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

	availableSymbols := []WhitebitMarketPair{}
	err = sonic.Unmarshal(data, &availableSymbols)
	if err != nil {
		return nil, err
	}
	return availableSymbols, nil

}

func (b *WhitebitClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing whitebit datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			symbol := model.ParseSymbol(v2.Name)
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(symbol.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(symbol.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  symbol.Base,
					Quote: symbol.Quote,
				},
				)
			}
		}
	}

	// batch subscriptions
	chunksize := len(subscribedSymbols)
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"id":     b.subscriptionId.Add(1),
			"method": "lastprice_subscribe",
			"params": []string{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = s
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *WhitebitClient) GetName() string {
	return b.name
}

func (b *WhitebitClient) setLastTickerWatcher() {
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

func (b *WhitebitClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.SendMessageJSON(websocket.TextMessage,
					map[string]interface{}{
						"id":     b.subscriptionId.Add(1),
						"method": "ping",
						"params": []string{},
					},
				); err != nil {
					b.log.Warn("Failed to send ping", "error", err)
				}

			}
		}
	}()
}
