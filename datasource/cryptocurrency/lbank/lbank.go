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
)

type LbankClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	subscriptionId atomic.Uint64
	tzInfo         *time.Location

	isRunning bool
}

func NewLbankClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*LbankClient, error) {
	wsEndpoint := "wss://www.lbkex.net/ws/V2/"

	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, multierror.Append(fmt.Errorf("error loading timezone information"), err)
	}

	lbank := LbankClient{
		name:         "lbank",
		log:          slog.Default().With(slog.String("datasource", "lbank")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
		tzInfo:       shanghaiTimezone,
	}
	lbank.wsClient.SetMessageHandler(lbank.onMessage)
	lbank.wsClient.SetOnConnect(lbank.onConnect)

	lbank.wsClient.SetLogger(lbank.log)
	lbank.log.Debug("Created new datasource")
	return &lbank, nil
}

func (d *LbankClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	d.wsClient.Start()

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *LbankClient) onConnect() error {
	err := d.SubscribeTickers()
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (d *LbankClient) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.wsClient.Close()
	d.isRunning = false
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
			d.lastTimestamp = time.Now()
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

	ticker, err := model.NewTicker(fmt.Sprintf("%.6f", newTickerEvent.Ticker.LastPrice),
		symbol,
		d.GetName(),
		ts)

	return ticker, err
}

func (d *LbankClient) SubscribeTickers() error {
	for _, v := range d.SymbolList {
		subMessage := map[string]interface{}{
			"action":    "subscribe",
			"subscribe": "tick",
			"pair":      fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		d.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		d.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	d.log.Debug("Subscribed ticker symbols")

	return nil
}

func (d *LbankClient) GetName() string {
	return d.name
}

func (d *LbankClient) setLastTickerWatcher() {
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
func (d *LbankClient) setPing() {
	ticker := time.NewTicker(time.Duration(d.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				id := d.subscriptionId.Add(1)
				msg := map[string]interface{}{
					"ping":   fmt.Sprintf("%d", id),
					"action": "ping",
				}
				if err := d.wsClient.SendMessageJSON(websocket.TextMessage, msg); err != nil {
					d.log.Warn("Failed to send ping", "error", err)
				}

			}
		}
	}()
}
