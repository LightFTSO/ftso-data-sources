package bitmart

import (
	"errors"
	"fmt"
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

type BitmartClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *broadcast.Broadcaster
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

func NewBitmartClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitmartClient, error) {
	wsEndpoint := "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"

	bitmart := BitmartClient{
		name:             "bitmart",
		log:              slog.Default().With(slog.String("datasource", "bitmart")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		SymbolList:       symbolList.Crypto,
		pingInterval:     15 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	bitmart.symbolChunks = bitmart.SymbolList.ChunkSymbols(100)
	bitmart.log.Debug("Created new datasource")
	return &bitmart, nil
}

func (d *BitmartClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)
	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, chunk)
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}
	d.setLastTickerWatcher()

	return nil
}

func (d *BitmartClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.isRunning = false
	d.clientClosedChan.Send(true)
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.W.Done()

	return nil
}

func (d *BitmartClient) IsRunning() bool {
	return d.isRunning
}

func (d *BitmartClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, `"event":"subscribe"`) {
			return
		}

		if strings.Contains(msg, `"table":"spot/ticker"`) {
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

func (d *BitmartClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.LastPrice,
			symbol,
			d.GetName(),
			time.UnixMilli(t.TimestampMs))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (d *BitmartClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	chunksize := 10
	for i := 0; i < len(symbols); i += chunksize {
		subMessage := map[string]interface{}{
			"op":   "subscribe",
			"args": []string{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(d.SymbolList) {
				continue
			}
			v := d.SymbolList[i+j]
			s = append(s, fmt.Sprintf("spot/ticker:%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["args"] = s

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))

	return nil
}

func (d *BitmartClient) GetName() string {
	return d.name
}

func (d *BitmartClient) setLastTickerWatcher() {
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

func (d *BitmartClient) setPing() {
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
					wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte("ping")})
				}
			}
		}
	}()
}
