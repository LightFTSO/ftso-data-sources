package kraken

import (
	"errors"
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
	"github.com/hashicorp/go-multierror"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type KrakenClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewKrakenClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*KrakenClient, error) {
	wsEndpoint := "wss://ws.kraken.com/v2"

	kraken := KrakenClient{
		name:         "kraken",
		log:          slog.Default().With(slog.String("datasource", "kraken")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.kraken.com/0",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}

	kraken.wsClient.SetMessageHandler(kraken.onMessage)
	kraken.wsClient.SetOnConnect(kraken.onConnect)
	kraken.wsClient.SetLogger(kraken.log)
	kraken.log.Debug("Created new datasource")
	return &kraken, nil
}

func (b *KrakenClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *KrakenClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (b *KrakenClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *KrakenClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "pong") {
			b.PongHandler(message.Message)
			return
		}
		if strings.Contains(string(message.Message), `"channel":"ticker"`) &&
			!strings.Contains(string(message.Message), `"method":"subscribe"`) {
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

func (b *KrakenClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent KrakenSnapshotUpdate
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		return &model.Ticker{}, err
	}

	tickerData := newTickerEvent.Data[0]
	symbol := model.ParseSymbol(tickerData.Symbol)

	base := KrakenAsset(symbol.Base)

	ticker, err := model.NewTicker(strconv.FormatFloat(tickerData.Last, 'f', 9, 64),
		model.Symbol{Base: string(base.GetStdName()),
			Quote: symbol.Quote},
		b.GetName(),
		time.Now())

	return ticker, err
}

func (b *KrakenClient) getAvailableSymbols() ([]AssetPairInfo, error) {
	reqUrl := b.apiEndpoint + "/public/AssetPairs"

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

	var exchangeInfo = new(ApiAssetPairResponse)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	if len(exchangeInfo.Error) > 0 {
		var apierrors error
		for _, apierr := range exchangeInfo.Error {
			multierror.Append(apierrors, errors.New(apierr))
		}
		return nil, apierrors
	}

	assetPairs := []AssetPairInfo{}

	for _, v := range exchangeInfo.Result {
		assetPairs = append(assetPairs, v)
	}

	return assetPairs, nil

}

func (b *KrakenClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		b.log.Error("error obtaining available symbols. Closing kraken datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []string{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(string(v2.Base.GetStdName()))) &&
				strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(string(v2.Quote.GetStdName()))) {

				subscribedSymbols = append(subscribedSymbols, (v2.WsName))
			}
		}
	}

	// batch subscriptions in packets of 5
	chunksize := 5
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "subscribe",
			"req_id": rand.Uint32(),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, v)
		}
		subMessage["params"] = map[string]interface{}{
			"channel":       "ticker",
			"event_trigger": "trades",
			"snapshot":      true,
			"symbol":        s,
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols", "symbols", len(subscribedSymbols))
	return nil
}

func (b *KrakenClient) GetName() string {
	return b.name
}

func (b *KrakenClient) setLastTickerWatcher() {
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

func (b *KrakenClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(`{"event":"ping"}`)})
		}
	}()
}

func (b *KrakenClient) PongHandler(msg []byte) {
	b.log.Debug("Pong received", "datasource", "kraken")
}
