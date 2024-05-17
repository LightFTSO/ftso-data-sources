package mexc

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

type MexcClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *broadcast.Broadcaster
	wsClient      internal.WebsocketClient
	wsEndpoint    string
	apiEndpoint   string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int

	tzInfo         *time.Location
	subscriptionId atomic.Uint64
}

func NewMexcClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*MexcClient, error) {
	wsEndpoint := "wss://wbs.mexc.com/ws"

	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, multierror.Append(fmt.Errorf("error loading timezone information"), err)
	}

	mexc := MexcClient{
		name:         "mexc",
		log:          slog.Default().With(slog.String("datasource", "mexc")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.mexc.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
		tzInfo:       shanghaiTimezone,
	}
	mexc.wsClient.SetMessageHandler(mexc.onMessage)

	mexc.wsClient.SetLogger(mexc.log)
	mexc.log.Debug("Created new datasource")
	return &mexc, nil
}

func (b *MexcClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Connect()
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *MexcClient) Reconnect() error {
	err := b.wsClient.Reconnect()
	if err != nil {
		return err
	}

	err = b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	b.SetPing()
	return nil
}
func (b *MexcClient) Close() error {
	b.wsClient.Disconnect()
	b.W.Done()

	return nil
}

func (b *MexcClient) onMessage(message internal.WsMessage) {
	if message.Err != nil {
		b.Reconnect()
		return
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"c":"spot@public.miniTicker`) {
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

func (b *MexcClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return &model.Ticker{}, err
	}

	symbol := model.ParseSymbol(newTickerEvent.Data.Symbol)
	ticker, err := model.NewTicker(newTickerEvent.Data.LastPrice,
		symbol,
		b.GetName(),
		time.UnixMilli(newTickerEvent.Timestamp))

	return ticker, err
}

func (b *MexcClient) SubscribeTickers() error {
	for _, v := range b.SymbolList {
		subMessage := map[string]interface{}{
			"id":     b.subscriptionId.Add(1),
			"method": "SUBSCRIPTION",
			"params": []string{fmt.Sprintf("spot@public.miniTicker.v3.api@%s%s@UTC+8", strings.ToUpper(v.Base), strings.ToUpper(v.Quote))},
		}
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
		b.log.Debug("Subscribed ticker symbol", "symbols", v.GetSymbol())
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *MexcClient) GetName() string {
	return b.name
}

func (b *MexcClient) setLastTickerWatcher() {
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
				b.Reconnect()
				return
			}
		}
	}()
}

func (b *MexcClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"method":"PING"}`)})
		}
	}()
}
