package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type OkxClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewOkxClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*OkxClient, error) {
	wsEndpoint := "wss://ws.okx.com:8443/ws/v5/public"

	okx := OkxClient{
		name:        "okx",
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:  wsEndpoint,
		SymbolList:  symbolList.Crypto,

		pingInterval: 29,
	}
	okx.wsClient.SetMessageHandler(okx.onMessage)

	log.Debug("Created new datasource", "datasource", okx.GetName())
	return &okx, nil
}

func (b *OkxClient) Connect() error {
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

func (b *OkxClient) Reconnect() error {
	log.Info("Reconnecting...", "datasource", b.GetName())

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
	return nil
}

func (b *OkxClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *OkxClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)

		if strings.Contains(msg, `"channel":"index-tickers"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"error", err.Error())
				return nil
			}
			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}

		}
	}

	return nil
}

func (b *OkxClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var tickerMessage OkxTicker
	err := json.Unmarshal(message, &tickerMessage)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
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
			b.GetName(),
			time.UnixMilli(ts))
		if err != nil {
			log.Error("Error parsing ticker", "datasource", b.GetName(),
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *OkxClient) SubscribeTickers() error {
	s := []map[string]interface{}{}
	for _, v := range b.SymbolList {
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

	b.wsClient.SendMessageJSON(subMessage)

	return nil
}

func (b *OkxClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`ping`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}

func (b *OkxClient) GetName() string {
	return b.name
}
