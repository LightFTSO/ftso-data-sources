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
	wsClient           *internal.WebSocketClient
	wsEndpoint         string
	SymbolList         []model.Symbol
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	isRunning bool
}

func NewBitmartClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitmartClient, error) {
	wsEndpoint := "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"

	bitmart := BitmartClient{
		name:         "bitmart",
		log:          slog.Default().With(slog.String("datasource", "bitmart")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15 * time.Second,
	}
	bitmart.wsClient.SetMessageHandler(bitmart.onMessage)
	bitmart.wsClient.SetOnConnect(bitmart.onConnect)

	bitmart.wsClient.SetLogger(bitmart.log)
	bitmart.log.Debug("Created new datasource")
	return &bitmart, nil
}

func (d *BitmartClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *BitmartClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (d *BitmartClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.isRunning = false
	d.wsClient.Close()
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

		// decompress
		/*decompressedData, err := internal.DecompressFlate(message.Message)
								if err != nil {
									d.log.Error("Error decompressing message", "error", err.Error())
									return nil
								}
								data := string(decompressedData)
								if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
									ticker, err := d.parseTicker([]byte(data))
									if err != nil {
										d.log.Error("Error parsing ticker",
						"ticker",ticker,"error", err.Error())
										return nil
									}
									d.lastTimestampMutex.Lock()
		    d.lastTimestamp = time.Now()
		   d.lastTimestampMutex.Unlock()

				d.TickerTopic.Send(ticker)
								}*/
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

func (d *BitmartClient) SubscribeTickers() error {
	// batch subscriptions in packets of 10
	chunksize := 10
	for i := 0; i < len(d.SymbolList); i += chunksize {
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
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols")

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

func (d *BitmartClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte("ping")})
		}
	}()
}
