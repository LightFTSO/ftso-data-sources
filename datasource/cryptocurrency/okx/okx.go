package okx

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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type OkxClient struct {
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

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewOkxClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*OkxClient, error) {
	wsEndpoint := "wss://ws.okx.com:8443/ws/v5/public"

	okx := OkxClient{
		name:        "okx",
		log:         slog.Default().With(slog.String("datasource", "okx")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,

		pingInterval:     29 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	okx.symbolChunks = okx.SymbolList.ChunkSymbols(1024)
	okx.log.Debug("Created new datasource")
	return &okx, nil
}

func (d *OkxClient) Connect() error {
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

func (d *OkxClient) Close() error {
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

func (d *OkxClient) IsRunning() bool {
	return d.isRunning
}

func (d *OkxClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"channel":"index-tickers"`) {
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

func (d *OkxClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var tickerMessage OkxTicker
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, v := range tickerMessage.Data {
		symbol := model.ParseSymbol(v.InstId)

		ts, err := strconv.ParseInt(v.Ts, 10, 64)
		if err != nil {
			return nil, err
		}

		newTicker, err := model.NewTickerPriceString(v.Idxpx,
			symbol,
			d.GetName(),
			time.UnixMilli(ts))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (d *OkxClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	s := []map[string]interface{}{}
	for _, v := range symbols {
		s = append(s, map[string]interface{}{
			"channel": "index-tickers",
			"instId": fmt.Sprintf("%s-%s",
				strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		})
	}
	subMessage := map[string]interface{}{
		"op":   "subscribe",
		"args": s,
	}

	wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	return nil
}

func (d *OkxClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("ping sender goroutine exiting")
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`ping`)})
				}
			}
		}
	}()
}

func (d *OkxClient) GetName() string {
	return d.name
}

func (d *OkxClient) setLastTickerWatcher() {
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
