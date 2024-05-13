package digifinex

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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

type DigifinexClient struct {
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

func NewDigifinexClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*DigifinexClient, error) {
	wsEndpoint := "wss://openapi.digifinex.com/ws/v1/"

	digifinex := DigifinexClient{
		name:         "digifinex",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://openapi.digifinex.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	digifinex.wsClient.SetMessageHandler(digifinex.onMessage)

	log.Debug("Created new datasource", "datasource", digifinex.GetName())
	return &digifinex, nil
}

func (b *DigifinexClient) Connect() error {
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

func (b *DigifinexClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())
	if b.cancel != nil {
		b.cancel()
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

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
func (b *DigifinexClient) Close() error {
	b.cancel()
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *DigifinexClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.BinaryMessage {
		/*if strings.Contains(msg, `"table":"spot/ticker"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"error", err.Error())
				return err
			}
			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}
		}*/

		// decompress
		decompressedData, err := internal.DecompressZlib(message.Message)
		if err != nil {
			log.Error("Error decompressing message", "datasource", b.GetName(), "error", err.Error())
			return nil
		}
		data := string(decompressedData)
		if strings.Contains(data, "ticker.update") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			tickers, err := b.parseTicker([]byte(data))
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil
			}
			for _, t := range tickers {
				b.TickerTopic.Send(t)
			}
		}
	}

	return nil
}

func (b *DigifinexClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Params {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.LastPrice,
			symbol,
			b.GetName(),
			time.UnixMilli(t.Timestamp))
		if err != nil {
			log.Error("Error parsing ticker", "datasource", b.GetName(),
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *DigifinexClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := b.apiEndpoint + "/v3/markets"

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

	var exchangeInfo = new(MarketInfo)
	err = sonic.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	availableMarkets := []model.Symbol{}
	for _, v := range exchangeInfo.Data {
		availableMarkets = append(availableMarkets, model.ParseSymbol(v.Market))
	}

	return availableMarkets, nil
}

func (b *DigifinexClient) SubscribeTickers() error {
	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		b.W.Done()
		log.Error("error obtaining available symbols. Closing kraken datasource", "datasource", b.GetName(), "error", err.Error())
		return err
	}

	subscribedSymbols := []string{}
	for _, v1 := range b.SymbolList {
		for _, v2 := range availableSymbols {
			if strings.EqualFold(strings.ToUpper(v1.Base), strings.ToUpper(string(v2.Base))) &&
				strings.EqualFold(strings.ToUpper(v1.Quote), strings.ToUpper(string(v2.Quote))) {

				subscribedSymbols = append(subscribedSymbols, fmt.Sprintf("%s_%s", strings.ToUpper(v2.Base), strings.ToUpper(v2.Quote)))
			}
		}
	}

	// batch subscriptions in packets of 20
	chunksize := 20
	for i := 0; i < len(subscribedSymbols); i += chunksize {
		subMessage := map[string]interface{}{
			"method": "ticker.subscribe",
			"id":     fmt.Sprint(rand.Uint32() % 999999),
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(subscribedSymbols) {
				continue
			}
			v := subscribedSymbols[i+j]
			s = append(s, v)
		}
		subMessage["params"] = s
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName(), "symbols", len(subscribedSymbols))
	return nil
}

func (b *DigifinexClient) GetName() string {
	return b.name
}

func (b *DigifinexClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				msg := map[string]interface{}{
					"ping":   fmt.Sprint(rand.Uint32() % 999999),
					"method": "ping",
					"params": []string{},
				}
				if err := b.wsClient.Connection.WriteJSON(msg); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
