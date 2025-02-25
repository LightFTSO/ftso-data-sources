package fmfw

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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type FmfwClient struct {
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

func NewFmfwClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*FmfwClient, error) {
	wsEndpoint := "wss://api.fmfw.io/api/3/ws/public"

	fmfw := FmfwClient{
		name:             "fmfw",
		log:              slog.Default().With(slog.String("datasource", "fmfw")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		SymbolList:       symbolList.Crypto,
		pingInterval:     15 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	fmfw.symbolChunks = fmfw.SymbolList.ChunkSymbols(1024)
	fmfw.log.Debug("Created new datasource")
	return &fmfw, nil
}

func (d *FmfwClient) Connect() error {
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

func (d *FmfwClient) Close() error {
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

func (d *FmfwClient) IsRunning() bool {
	return d.isRunning
}

func (d *FmfwClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "ticker/price/1s") && strings.Contains(string(message.Message), "data") {
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

func (d *FmfwClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	keys := make([]string, 0, len(newTickerEvent.Data))
	for k := range newTickerEvent.Data {
		keys = append(keys, k)
	}

	tickers := []*model.Ticker{}
	for _, key := range keys {
		tickData := newTickerEvent.Data[key]
		symbol := model.ParseSymbol(key)
		newTicker, err := model.NewTicker(tickData.LastPrice,
			symbol,
			d.GetName(),
			time.UnixMilli(tickData.Timestamp))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (d *FmfwClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// batch subscriptions in packets
	chunksize := len(symbols)
	for i := 0; i < len(symbols); i += chunksize {
		subMessage := map[string]interface{}{
			"ch":     "ticker/price/1s/batch",
			"method": "subscribe",
			"id":     time.Now().UnixMicro(),
			"params": map[string]interface{}{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(symbols) {
				continue
			}
			v := d.SymbolList[i+j]
			s = append(s, fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"symbols": s,
		}

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))

	return nil
}

func (d *FmfwClient) GetName() string {
	return d.name
}

func (d *FmfwClient) setLastTickerWatcher() {
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

func (d *FmfwClient) setPing() {
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
					wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte("ping")})
				}
			}
		}
	}()
}
