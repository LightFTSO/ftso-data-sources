package kraken

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/go-multierror"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type KrakenClient struct {
	name        string
	W           *sync.WaitGroup
	TradeTopic  *broadcast.Broadcaster
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewKrakenClient(options interface{}, symbolList symbols.AllSymbols, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*KrakenClient, error) {
	log.Info("Created new datasource", "datasource", "kraken")
	wsEndpoint := "wss://ws.kraken.com/"

	kraken := KrakenClient{
		name:         "kraken",
		W:            w,
		TradeTopic:   tradeTopic,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.kraken.com/0",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}

	return &kraken, nil
}

func (b *KrakenClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to kraken datasource")

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	b.wsClient.SetMessageHandler(b.onMessage)
	b.SetPing()

	go b.wsClient.Listen()

	return nil
}

func (b *KrakenClient) Reconnect() error {
	log.Info("Reconnecting to kraken datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected to kraken datasource")
	err = b.SubscribeTrades()
	if err != nil {
		log.Error("Error subscribing to trades", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}
func (b *KrakenClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *KrakenClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket data, reconnecting in 5 seconds",
			"datasource", b.GetName(), "error", message.Err)
		time.Sleep(1 * time.Second)
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "pong") {
			b.PongHandler(message.Message)
			return nil
		}

		if strings.Contains(string(message.Message), "trade") &&
			!strings.Contains(string(message.Message), `"channelName":"trade","event":"subscriptionStatus"`) {
			trades, err := b.parseTrade(message.Message)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())

			}
			for _, v := range trades {
				b.TradeTopic.Send(v)

			}
			return nil
		}

	}

	return nil
}

func (b *KrakenClient) parseTrade(message []byte) ([]*model.Trade, error) {
	var newTradeEvent WsTradeMessage
	err := json.Unmarshal(message, &newTradeEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Trade{}, err
	}

	trades := []*model.Trade{}
	pair := newTradeEvent[3]
	for _, tr := range newTradeEvent[1].([]interface{}) {
		tr := tr.([]interface{})

		ts, err := strconv.ParseInt(strings.Replace(tr[2].(string), ".", "", 1), 10, 64)
		if err != nil {
			return nil, err
		}
		symbol := model.ParseSymbol(pair.(string))
		var base = KrakenAsset(symbol.Base)
		trades = append(trades, &model.Trade{
			Base:      string(base.GetStdName()),
			Quote:     symbol.Quote,
			Symbol:    string(base.GetStdName()) + "/" + symbol.Quote,
			Price:     tr[0].(string),
			Size:      tr[0].(string),
			Source:    b.GetName(),
			Timestamp: time.UnixMicro(ts),
		})
	}

	return trades, nil
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
	err = json.Unmarshal(data, exchangeInfo)
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

func (b *KrakenClient) SubscribeTrades() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		log.Error("error obtaining available symbols. Closing kraken datasource", "error", err.Error())
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
			"event": "subscribe",
			"reqid": rand.Uint32(),
			"subscription": map[string]interface{}{
				"name": "trade",
				//"snapshot": false,
			},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, v)
		}
		subMessage["pair"] = s
		//fmt.Println(subMessage)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed trade symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *KrakenClient) SubscribeTickers() error {
	return nil
}

func (b *KrakenClient) GetName() string {
	return b.name
}

func (b *KrakenClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.TextMessage, []byte(`{"event":"ping"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}

func (b *KrakenClient) PongHandler(msg []byte) {
	log.Debug("Pong received", "datasource", "kraken")
}
