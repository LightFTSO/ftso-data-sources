package bitrue

import (
	"errors"
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
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	isRunning bool
}

func NewBitrueClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitrueClient, error) {

	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := BitrueClient{
		name:         "bitrue",
		log:          slog.Default().With(slog.String("datasource", "bitrue")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bitrue.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bitrue.wsClient.SetMessageHandler(bitrue.onMessage)
	bitrue.wsClient.SetOnConnect(bitrue.onConnect)

	bitrue.wsClient.SetLogger(bitrue.log)
	bitrue.log.Debug("Created new datasource")
	return &bitrue, nil
}

func (d *BitrueClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setLastTickerWatcher()

	return nil
}

func (d *BitrueClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (d *BitrueClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.W.Done()
	d.isRunning = false

	return nil
}

func (d *BitrueClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitrueClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		// decompress
		decompressedData, err := internal.DecompressGzip(message.Message)
		if err != nil {
			d.log.Error("Error decompressing message", "error", err.Error())
			return
		}
		data := string(decompressedData)
		if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			ticker, err := d.parseTicker([]byte(data))
			if err != nil {
				d.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			d.lastTimestamp = time.Now()
			d.TickerTopic.Send(ticker)
			return
		}

		if strings.Contains(data, "ping") {
			pong := strings.ReplaceAll(data, "ping", "pong")
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(pong)})
			d.log.Debug("Pong received")
			return
		}
	}
}

func (d *BitrueClient) parseTicker(message []byte) (*model.Ticker, error) {
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
		d.GetName(),
		time.UnixMilli(int64(newEvent.Timestamp)))

	return newTicker, err
}

func (d *BitrueClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		cb_id := fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"event": "sub",
			"params": map[string]interface{}{
				"channel": fmt.Sprintf("market_%s_ticker", cb_id),
				"cb_id":   cb_id,
			},
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	return nil
}

func (d *BitrueClient) GetName() string {
	return d.name
}

func (d *BitrueClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(d.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				d.lastTimestamp = time.Now()
				d.wsClient.Reconnect()
			}
		}
	}()
}
