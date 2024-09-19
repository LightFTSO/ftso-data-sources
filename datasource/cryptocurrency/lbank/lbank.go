package lbank

import (
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

func (b *LbankClient) Connect() error {
	b.isRunning = true
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *LbankClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}
	return nil
}
func (b *LbankClient) Close() error {
	b.wsClient.Close()
	b.isRunning = false
	b.W.Done()

	return nil
}

func (b *LbankClient) IsRunning() bool {
	return b.isRunning
}

func (b *LbankClient) onMessage(message internal.WsMessage) {
	msg := string(message.Message)

	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, `"type":"tick"`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"ticker", ticker, "error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()
			b.TickerTopic.Send(ticker)

		}
	}
}

func (b *LbankClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Pair)
	ts, err := time.ParseInLocation("2006-01-02T15:04:05.999", newTickerEvent.Timestamp, b.tzInfo)
	if err != nil {
		return nil, err
	}

	ticker, err := model.NewTicker(fmt.Sprintf("%.6f", newTickerEvent.Ticker.LastPrice),
		symbol,
		b.GetName(),
		ts)

	return ticker, err
}

func (b *LbankClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"action":    "subscribe",
			"subscribe": "tick",
			"pair":      fmt.Sprintf("%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *LbankClient) GetName() string {
	return b.name
}

func (b *LbankClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	b.lastTimestamp = time.Now()
	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for range lastTickerIntervalTimer.C {
			now := time.Now()
			diff := now.Sub(b.lastTimestamp)
			if diff > timeout {
				// no tickers received in a while, attempt to reconnect
				b.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
				b.lastTimestamp = time.Now()
				b.wsClient.Reconnect()
			}
		}
	}()
}
func (b *LbankClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				id := b.subscriptionId.Add(1)
				msg := map[string]interface{}{
					"ping":   fmt.Sprintf("%d", id),
					"action": "ping",
				}
				if err := b.wsClient.SendMessageJSON(websocket.TextMessage, msg); err != nil {
					b.log.Warn("Failed to send ping", "error", err)
				}

			}
		}
	}()
}
