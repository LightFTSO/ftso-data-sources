package mexc

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

type MexcClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
	wsClient           *internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	tzInfo         *time.Location
	subscriptionId atomic.Uint64
	isRunning      bool
}

func NewMexcClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*MexcClient, error) {
	wsEndpoint := "wss://wbs.mexc.com/ws"

	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, fmt.Errorf("error loading timezone information: %w", err)
	}

	mexc := MexcClient{
		name:         "mexc",
		log:          slog.Default().With(slog.String("datasource", "mexc")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.mexc.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20 * time.Second,
		tzInfo:       shanghaiTimezone,
	}
	mexc.wsClient.SetMessageHandler(mexc.onMessage)
	mexc.wsClient.SetOnConnect(mexc.onConnect)

	mexc.wsClient.SetLogger(mexc.log)
	mexc.log.Debug("Created new datasource")
	return &mexc, nil
}

func (d *MexcClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *MexcClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (d *MexcClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *MexcClient) IsRunning() bool {
	return d.isRunning
}

func (d *MexcClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"c":"spot@public.miniTicker`) {
			ticker, err := d.parseTicker(message.Message)
			if err != nil {
				d.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			d.TickerTopic.Send(ticker)
		}
	}
}

func (d *MexcClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error("Error unmarshaling ticker", "error", err)
		return nil, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		d.GetName(),
		time.UnixMilli(newTickerEvent.Timestamp))
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}

	return ticker, err
}

func (d *MexcClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "SUBSCRIPTION",
			"params": []string{fmt.Sprintf("spot@public.miniTicker.v3.api@%s%s@UTC+8", strings.ToUpper(v.Base), strings.ToUpper(v.Quote))},
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	d.log.Debug("Subscribed ticker symbols")

	return nil
}

func (d *MexcClient) GetName() string {
	return d.name
}

func (d *MexcClient) setLastTickerWatcher() {
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

func (d *MexcClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"method":"PING"}`)})
		}
	}()
}
