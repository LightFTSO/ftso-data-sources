package bybit

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
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

type BybitClient struct {
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

func NewBybitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BybitClient, error) {
	wsEndpoint := "wss://stream.bybit.com/v5/public/spot"

	bybit := BybitClient{
		name:         "bybit",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bybit.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bybit.wsClient.SetMessageHandler(bybit.onMessage)

	log.Debug("Created new datasource", "datasource", bybit.GetName())
	return &bybit, nil
}

func (b *BybitClient) Connect() error {
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

func (b *BybitClient) Reconnect() error {
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
func (b *BybitClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BybitClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "tickers.") {
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

func (b *BybitClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
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
		log.Error("error obtaining available symbols. Closing bybit datasource", "error", err.Error())
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
		//fmt.Println(subMessage)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *BybitClient) GetName() string {
	return b.name
}

func (b *BybitClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`{"op":"ping"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
