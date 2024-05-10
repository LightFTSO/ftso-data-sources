package bitrue

import (
	"context"
	"fmt"
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

type BitrueClient struct {
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

func NewBitrueClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitrueClient, error) {

	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := BitrueClient{
		name:         "bitrue",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bitrue.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bitrue.wsClient.SetMessageHandler(bitrue.onMessage)

	log.Debug("Created new datasource", "datasource", bitrue.GetName())
	return &bitrue, nil
}

func (b *BitrueClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *BitrueClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())

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
func (b *BitrueClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BitrueClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.BinaryMessage {
		// decompress
		compressedData, err := internal.DecompressGzip(message.Message)
		if err != nil {
			log.Error("Error decompressing message", "datasource", b.GetName(), "error", err.Error())
			return nil
		}
		data := string(compressedData)
		if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			ticker, err := b.parseTicker([]byte(data))
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"ticker", ticker, "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
			return nil
		}

		if strings.Contains(data, "ping") {
			pong := strings.ReplaceAll(data, "ping", "pong")
			b.wsClient.SendMessage([]byte(pong))
			log.Debug("Pong received", "datasource", b.GetName())
			return nil
		}
	}

	return nil
}

func (b *BitrueClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newEvent TickerResponse
	err := sonic.Unmarshal(message, &newEvent)
	if err != nil {
		return &model.Ticker{}, err
	}

	pair := strings.ReplaceAll(newEvent.Channel, "market_", "")
	pair = strings.ReplaceAll(pair, "_ticker", "")
	symbol := model.ParseSymbol(pair)

	newTicker, err := model.NewTicker(strconv.FormatFloat(newEvent.TickData.Close, 'f', 9, 64),
		symbol,
		b.GetName(),
		time.UnixMilli(int64(newEvent.Timestamp)))

	return newTicker, err
}

func (b *BitrueClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		cb_id := fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"event": "sub",
			"params": map[string]interface{}{
				"channel": fmt.Sprintf("market_%s_ticker", cb_id),
				"cb_id":   cb_id,
			},
		}
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed ticker symbol", "datasource", b.GetName(), "symbols", v.GetSymbol())
	}

	return nil
}

func (b *BitrueClient) GetName() string {
	return b.name
}
