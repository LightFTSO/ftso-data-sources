package gateio

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type GateIoClient struct {
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
}

func NewGateIoClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*GateIoClient, error) {
	wsEndpoint := "wss://api.gateio.ws/ws/v4/"

	gateio := GateIoClient{
		name:         "gateio",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.gateio.ws/api/v4",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	gateio.wsClient.SetMessageHandler(gateio.onMessage)

	log.Debug("Created new datasource", "datasource", gateio.GetName())
	return &gateio, nil
}

func (b *GateIoClient) Connect() error {
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

func (b *GateIoClient) Reconnect() error {
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
func (b *GateIoClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *GateIoClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "spot.tickers") && strings.Contains(string(message.Message), "\"event\":\"update\"") {
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

func (b *GateIoClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Result.CurrencyPair)
	ticker, err := model.NewTicker(newTickerEvent.Result.Last,
		symbol,
		b.GetName(),
		time.UnixMilli(newTickerEvent.TimeMs))

	return ticker, err
}

func (b *GateIoClient) getAvailableSymbols() (*[]GateIoInstrument, error) {
	reqUrl := b.apiEndpoint + "/spot/currency_pairs"

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

	var availableSymbols = new([]GateIoInstrument)
	err = sonic.Unmarshal(data, availableSymbols)
	if err != nil {
		return nil, err
	}
	return availableSymbols, nil

}

func (b *GateIoClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		log.Error("error obtaining available symbols. Closing gateio datasource", "error", err.Error())
		return err
	}

	subscribedSymbols := []model.Symbol{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range *availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(v2.Base)) && strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(v2.Quote)) {
				subscribedSymbols = append(subscribedSymbols, model.Symbol{
					Base:  v2.Base,
					Quote: v2.Quote})
			}
		}
	}

	// batch subscriptions in packets of 10
	chunksize := 10
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"time":    time.Now().UnixMilli(),
			"channel": "spot.tickers",
			"event":   "subscribe",
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["payload"] = s
		//fmt.Println(subMessage)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *GateIoClient) GetName() string {
	return b.name
}

func (b *GateIoClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`{"method":"server.ping"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
