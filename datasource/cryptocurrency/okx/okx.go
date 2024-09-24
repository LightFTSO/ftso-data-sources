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
)

type OkxClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	isRunning bool
}

func NewOkxClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*OkxClient, error) {
	wsEndpoint := "wss://ws.okx.com:8443/ws/v5/public"

	okx := OkxClient{
		name:        "okx",
		log:         slog.Default().With(slog.String("datasource", "okx")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,

		pingInterval: 29,
	}
	okx.wsClient.SetMessageHandler(okx.onMessage)
	okx.wsClient.SetOnConnect(okx.onConnect)

	okx.wsClient.SetLogger(okx.log)
	okx.log.Debug("Created new datasource")
	return &okx, nil
}

func (d *OkxClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)
	d.wsClient.Start()
	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *OkxClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}

func (d *OkxClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
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
			d.lastTimestamp = time.Now()

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

		newTicker, err := model.NewTicker(v.Idxpx,
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

func (d *OkxClient) SubscribeTickers() error {
	s := []map[string]interface{}{}
	for _, v := range d.SymbolList {
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

	d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)

	return nil
}

func (d *OkxClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			d.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`ping`)})
		}
	}()
}

func (d *OkxClient) GetName() string {
	return d.name
}

func (d *OkxClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(d.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				d.lastTimestamp = time.Now()
				d.wsClient.Reconnect()
			}
		}
	}()
}
