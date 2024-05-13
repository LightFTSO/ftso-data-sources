package mexc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/go-multierror"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type MexcClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc

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
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.mexc.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
		tzInfo:       shanghaiTimezone,
	}
	mexc.wsClient.SetMessageHandler(mexc.onMessage)

	log.Debug("Created new datasource", "datasource", mexc.GetName())
	return &mexc, nil
}

func (b *MexcClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting...", "datasource", b.GetName())

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()
	b.SetPing()

	return nil
}

func (b *MexcClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())
	if b.cancel != nil {
		b.cancel()
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected", "datasource", b.GetName())
	err = b.SubscribeTickers()
	if err != nil {
		log.Error("Error subscribing to tickers", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	b.SetPing()
	return nil
}
func (b *MexcClient) Close() error {
	b.cancel()
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *MexcClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), `"c":"spot@public.miniTicker`) {
			ticker, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"ticker", ticker, "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}
	}

	return nil
}

func (b *MexcClient) parseTicker(message []byte) (*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
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
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed ticker symbol", "datasource", b.GetName(), "symbols", v.GetSymbol())
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *MexcClient) GetName() string {
	return b.name
}

func (b *MexcClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`{"method":"PING"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
