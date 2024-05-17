package bitrue

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BitrueClient struct {
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
}

func NewBitrueClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitrueClient, error) {

	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := BitrueClient{
		name:         "bitrue",
		log:          slog.Default().With(slog.String("datasource", "bitrue")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bitrue.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bitrue.wsClient.SetMessageHandler(bitrue.onMessage)

	bitrue.wsClient.SetLogger(bitrue.log)
	bitrue.log.Debug("Created new datasource")
	return &bitrue, nil
}

func (b *BitrueClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.setLastTickerWatcher()

	return nil
}

func (b *BitrueClient) Reconnect() error {

	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (b *BitrueClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *BitrueClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
	}

	if message.Type == websocket.BinaryMessage {
		// decompress
		decompressedData, err := internal.DecompressGzip(message.Message)
		if err != nil {
			b.log.Error("Error decompressing message", "error", err.Error())
			return
		}
		data := string(decompressedData)
		if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			ticker, err := b.parseTicker([]byte(data))
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)
			return
		}

		if strings.Contains(data, "ping") {
			pong := strings.ReplaceAll(data, "ping", "pong")
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(pong)})
			b.log.Debug("Pong received")
			return
		}
	}
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
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	return nil
}

func (b *BitrueClient) GetName() string {
	return b.name
}

func (b *BitrueClient) setLastTickerWatcher() {
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
