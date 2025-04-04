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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type BitrueClient struct {
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

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewBitrueClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitrueClient, error) {

	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := BitrueClient{
		name:             "bitrue",
		log:              slog.Default().With(slog.String("datasource", "bitrue")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.bitrue.com",
		SymbolList:       symbolList.Crypto,
		pingInterval:     20 * time.Second,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	bitrue.symbolChunks = bitrue.SymbolList.ChunkSymbols(2048)
	bitrue.log.Debug("Created new datasource")
	return &bitrue, nil
}

func (d *BitrueClient) Connect() error {
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
	d.setLastTickerWatcher()

	return nil
}

func (d *BitrueClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.W.Done()
	d.isRunning = false
	d.clientClosedChan.Send(true)

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
			d.lastTimestampMutex.Lock()
			d.lastTimestamp = time.Now()
			d.lastTimestampMutex.Unlock()

			d.TickerTopic.Send(ticker)
			return
		}

		if strings.Contains(data, "ping") {
			pong := strings.ReplaceAll(data, "ping", "pong")
			for _, wsClient := range d.wsClients {
				wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(pong)})
			}
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
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return newTicker, err
}

func (d *BitrueClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		cb_id := fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"event": "sub",
			"params": map[string]interface{}{
				"channel": fmt.Sprintf("market_%s_ticker", cb_id),
				"cb_id":   cb_id,
			},
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))

	return nil
}

func (d *BitrueClient) GetName() string {
	return d.name
}

func (d *BitrueClient) setLastTickerWatcher() {
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
