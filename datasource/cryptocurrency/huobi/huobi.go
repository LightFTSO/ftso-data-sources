package huobi

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

type HuobiClient struct {
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
	isRunning          bool
}

func NewHuobiClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*HuobiClient, error) {
	wsEndpoint := "wss://api.huobi.pro/ws"

	huobi := HuobiClient{
		name:        "huobi",
		log:         slog.Default().With(slog.String("datasource", "huobi")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClients:   []*internal.WebSocketClient{},
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,
	}
	huobi.symbolChunks = huobi.SymbolList.ChunkSymbols(1024)
	huobi.log.Debug("Created new datasource")
	return &huobi, nil
}

func (d *HuobiClient) Connect() error {
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

func (d *HuobiClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.W.Done()

	return nil
}

func (d *HuobiClient) IsRunning() bool {
	return d.isRunning
}

func (d *HuobiClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.BinaryMessage {
		// decompress
		decompressedData, err := internal.DecompressGzip(message.Message)
		if err != nil {
			d.log.Error("Error parsing binary message", "error", err.Error())
		}

		msg := string(decompressedData)
		if strings.Contains(msg, "ping") {
			for _, wsClient := range d.wsClients {
				wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte(strings.ReplaceAll(msg, "ping", "pong"))})
			}
			return
		}

		if (!strings.Contains(msg, "market.") && !strings.Contains(msg, ".ticker")) || !strings.Contains(msg, "open") {
			return
		}

		ticker, err := d.parseTicker(decompressedData)
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

func (d *HuobiClient) parseTicker(message []byte) (*model.Ticker, error) {
	var tickerMessage HuobiTicker
	err := sonic.Unmarshal(message, &tickerMessage)
	if err != nil {
		return &model.Ticker{}, err
	}

	market := strings.ReplaceAll(tickerMessage.Channel, "market.", "")
	market = strings.ReplaceAll(market, ".ticker", "")

	symbol := model.ParseSymbol(market)

	ticker, err := model.NewTicker(fmt.Sprint(tickerMessage.Tick.LastPrice),
		symbol,
		d.GetName(),
		time.UnixMilli(tickerMessage.Timestamp))
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *HuobiClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		subMessage := map[string]interface{}{
			"sub": fmt.Sprintf("market.%s%s.ticker", strings.ToLower(v.Base), strings.ToLower(v.Quote)),
			"id":  time.Now().UnixMilli(),
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))
	return nil
}

func (d *HuobiClient) GetName() string {
	return d.name
}

func (d *HuobiClient) setLastTickerWatcher() {
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

				for _, wsClient := range d.wsClients {
					wsClient.Reconnect()
				}
			}
		}
	}()
}
