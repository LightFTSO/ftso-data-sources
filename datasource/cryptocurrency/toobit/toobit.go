package toobit

import (
	"errors"
	"fmt"
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

type ToobitClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
	wsClient           *internal.WebSocketClient
	wsEndpoint         string
	SymbolList         []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	subscriptionId atomic.Uint64

	isRunning bool
}

func NewToobitClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*ToobitClient, error) {
	wsEndpoint := "wss://stream.toobit.com/quote/ws/v1"

	toobit := ToobitClient{
		name:         "toobit",
		log:          slog.Default().With(slog.String("datasource", "toobit")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 60 * time.Second,
	}
	toobit.wsClient.SetMessageHandler(toobit.onMessage)
	toobit.wsClient.SetOnConnect(toobit.onConnect)

	toobit.wsClient.SetLogger(toobit.log)
	toobit.log.Debug("Created new datasource")
	return &toobit, nil
}

func (d *ToobitClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *ToobitClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (d *ToobitClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *ToobitClient) IsRunning() bool {
	return d.isRunning
}

func (d *ToobitClient) onMessage(message internal.WsMessage) {
	msg := string(message.Message)
	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, "realtimes") {
			tickers, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"error", err.Error())
				return
			}
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			for _, v := range tickers {
				d.TickerTopic.Send(v)
			}
		}
	}
}

func (d *ToobitClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.Close,
			symbol,
			d.GetName(),
			time.UnixMilli(t.Timestamp))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (d *ToobitClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"topic": "realtimes",
			"event": "sub",
			"params": map[string]interface{}{
				"binary": false,
			},
			"symbol": fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	d.log.Debug("Subscribed ticker symbols")

	return nil
}

func (d *ToobitClient) GetName() string {
	return d.name
}

func (d *ToobitClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			d.lastTimestampMutex.Lock()
			diff := now.Sub(d.lastTimestamp)
			d.lastTimestampMutex.Unlock()

			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.lastTimestampMutex.Lock()
				d.lastTimestamp = time.Now()
				d.lastTimestampMutex.Unlock()

				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))

				d.wsClient.Reconnect()
			}
		}
	}()
}

func (d *ToobitClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := d.wsClient.SendMessageJSON(websocket.TextMessage,
					map[string]interface{}{
						"ping": d.subscriptionId.Add(1),
					},
				); err != nil {
					d.log.Warn("Failed to send ping", "error", err)
				}

			}
		}
	}()
}
