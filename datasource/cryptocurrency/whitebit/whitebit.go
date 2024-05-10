package whitebit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "log/slog"

	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type WhitebitClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc

	subscriptionId atomic.Uint64
}

func NewWhitebitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*WhitebitClient, error) {
	wsEndpoint := "wss://api.whitebit.com/ws"

	whitebit := WhitebitClient{
		name:         "whitebit",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://whitebit.com/api/v4/",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	whitebit.wsClient.SetMessageHandler(whitebit.onMessage)

	log.Debug("Created new datasource", "datasource", whitebit.GetName())
	return &whitebit, nil
}

func (b *WhitebitClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()
	b.SetPing()

	return nil
}

func (b *WhitebitClient) Reconnect() error {
	log.Info("Reconnecting...")

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
func (b *WhitebitClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *WhitebitClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	msg := string(message.Message)
	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, "lastprice_update") {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"ticker", ticker, "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *WhitebitClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
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
	err = json.Unmarshal(data, &availableSymbols)
	if err != nil {
		return nil, err
	}
	return availableSymbols, nil

}

func (b *WhitebitClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		log.Error("error obtaining available symbols. Closing whitebit datasource", "error", err.Error())
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
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *WhitebitClient) GetName() string {
	return b.name
}

func (b *WhitebitClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.Connection.WriteJSON(
					map[string]interface{}{
						"id":     b.subscriptionId.Add(1),
						"method": "ping",
						"params": []string{},
					},
				); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
