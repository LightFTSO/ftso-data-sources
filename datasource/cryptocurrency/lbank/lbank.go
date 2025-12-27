package lbank

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
	"github.com/hashicorp/go-multierror"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type LbankClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *tickertopic.TickerTopic
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	subscriptionId atomic.Uint64
	tzInfo         *time.Location

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewLbankClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*LbankClient, error) {
	wsEndpoint := "wss://www.lbkex.net/ws/V2/"

	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, multierror.Append(fmt.Errorf("error loading timezone information"), err)
	}

	lbank := LbankClient{
		name:             "lbank",
		log:              slog.Default().With(slog.String("datasource", "lbank")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		SymbolList:       symbolList.Crypto,
		pingInterval:     30 * time.Second,
		tzInfo:           shanghaiTimezone,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	lbank.symbolChunks = lbank.SymbolList.ChunkSymbols(1024)
	lbank.log.Debug("Created new datasource")
	return &lbank, nil
}

func (d *LbankClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			err := d.SubscribeTickers(wsClient, chunk)
			if err != nil {
				d.log.Error("Error subscribing to tickers")
				return err
			}
			return err
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *LbankClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.W.Done()

	return nil
}

func (d *LbankClient) IsRunning() bool {
	return d.isRunning
}

func (d *LbankClient) onMessage(message internal.WsMessage) {
	msg := string(message.Message)

	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, `"type":"tick"`) {
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

func (d *LbankClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Pair)
	ts, err := time.ParseInLocation("2006-01-02T15:04:05.999", newTickerEvent.Timestamp, d.tzInfo)
	if err != nil {
		return nil, err
	}

	ticker, err := model.NewTicker(newTickerEvent.Ticker.LastPrice,
		symbol,
		d.GetName(),
		ts)
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *LbankClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		subMessage := map[string]interface{}{
			"action":    "subscribe",
			"subscribe": "tick",
			"pair":      fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))
	return nil
}

func (d *LbankClient) GetName() string {
	return d.name
}

func (d *LbankClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("last ticker received watcher goroutine exiting")
				return
			case <-lastTickerIntervalTimer.C:
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

					for _, wsClient := range d.wsClients {
						wsClient.Reconnect()
					}
				}
			}
		}
	}()
}
func (d *LbankClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					id := d.subscriptionId.Add(1)
					msg := map[string]interface{}{
						"ping":   fmt.Sprintf("%d", id),
						"action": "ping",
					}
					if err := wsClient.SendMessageJSON(websocket.TextMessage, msg); err != nil {
						d.log.Warn("Failed to send ping", "error", err)
					}
				}
			}
		}
	}()
}
