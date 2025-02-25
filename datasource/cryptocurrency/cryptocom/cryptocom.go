package cryptocom

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type CryptoComClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *tickertopic.TickerTopic
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	subscriptionId atomic.Uint64

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewCryptoComClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*CryptoComClient, error) {
	wsEndpoint := "wss://stream.crypto.com/v2/market"

	cryptocom := CryptoComClient{
		name:             "cryptocom",
		log:              slog.Default().With(slog.String("datasource", "cryptocom")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.crypto.com/v2",
		SymbolList:       symbolList.Crypto,
		pingInterval:     20 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	cryptocom.symbolChunks = cryptocom.SymbolList.ChunkSymbols(399)
	cryptocom.log.Debug("Created new datasource")
	return &cryptocom, nil
}

func (d *CryptoComClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			time.Sleep(time.Second)
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
	d.setLastTickerWatcher()

	return nil
}

func (d *CryptoComClient) Close() error {
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

func (d *CryptoComClient) IsRunning() bool {
	return d.isRunning
}

func (d *CryptoComClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, "public/heartbeat") {
			d.pong(message.Message)
			return
		}

		if strings.Contains(msg, "\"channel\":\"ticker\"") && strings.Contains(msg, "\"subscription\":\"ticker.") {
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

func (d *CryptoComClient) parseTicker(message []byte) ([]*model.Ticker, error) {

	var tickerMessage WsTickerMessage
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		d.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	symbol := model.ParseSymbol(tickerMessage.Result.IntrumentName)
	tickers := []*model.Ticker{}
	for _, v := range tickerMessage.Result.Data {
		// some messages come with null data
		if v.LastPrice == "" {
			continue
		}

		newTicker, err := model.NewTicker(v.LastPrice,
			symbol,
			d.GetName(),
			time.UnixMilli(v.Timestamp))
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (d *CryptoComClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	// batch subscriptions in packets of 5
	chunksize := 10
	for i := 0; i < len(symbols); i += chunksize {
		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "subscribe",
			"nonce":  time.Now().UnixMicro(),
			"params": map[string]interface{}{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(d.SymbolList) {
				continue
			}
			v := d.SymbolList[i+j]
			s = append(s, fmt.Sprintf("ticker.%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"channels": s,
		}

		// sleep a bit to avoid rate limits
		time.Sleep(20 * time.Millisecond)
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))

	return nil
}

func (d *CryptoComClient) GetName() string {
	return d.name
}

func (d *CryptoComClient) setLastTickerWatcher() {
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

func (d *CryptoComClient) pong(pingMessage []byte) {
	d.log.Debug("Sending pong message")
	var ping PublicHeartbeat
	err := sonic.Unmarshal(pingMessage, &ping)
	if err != nil {
		d.log.Error(err.Error())
		return
	}

	pong := ping
	pong.Method = "public/respond-heartbeat"

	for _, wsClient := range d.wsClients {
		if err := wsClient.SendMessageJSON(websocket.TextMessage, pong); err != nil {
			d.log.Warn("Failed to send ping", "error", err)
			continue
		}
	}

}
