package bitget

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/gorilla/websocket"
	json "github.com/json-iterator/go"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type BitgetClient struct {
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

func NewBitgetClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitgetClient, error) {
	wsEndpoint := "wss://ws.bitget.com/v2/ws/public"

	bitget := BitgetClient{
		name:         "bitget",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 30,
	}
	bitget.wsClient.SetMessageHandler(bitget.onMessage)

	log.Debug("Created new datasource", "datasource", bitget.GetName())
	return &bitget, nil
}

func (b *BitgetClient) Connect() error {
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

func (b *BitgetClient) Reconnect() error {
	log.Info("Reconnecting...")

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
func (b *BitgetClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BitgetClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	msg := string(message.Message)
	if message.Type == websocket.TextMessage {
		if strings.Contains(msg, `"action":"snapshot"`) && strings.Contains(msg, `"channel":"ticker"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil
			}
			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}
		}
	}

	return nil
}

func (b *BitgetClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent WsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.InstId)
		ticker := model.Ticker{
			Base:      symbol.Base,
			Quote:     symbol.Quote,
			Symbol:    symbol.Symbol,
			LastPrice: t.LastPrice,
			Source:    b.GetName(),
			Timestamp: time.UnixMilli(newTickerEvent.Timestamp),
		}
		tickers = append(tickers, &ticker)
	}

	return tickers, nil
}

func (b *BitgetClient) SubscribeTickers() error {
	// batch subscriptions in packets of 5
	chunksize := 20
	for i := 0; i < len(b.SymbolList); i += chunksize {
		subMessage := map[string]interface{}{
			"op": "subscribe",
		}
		s := []interface{}{}
		for j := range chunksize {
			if i+j >= len(b.SymbolList) {
				continue
			}
			v := b.SymbolList[i+j]
			s = append(s, map[string]interface{}{
				"instType": "SPOT",
				"channel":  "ticker",
				"instId":   fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)),
			})
		}
		subMessage["args"] = s
		//fmt.Println(subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(100 * time.Millisecond)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *BitgetClient) GetName() string {
	return b.name
}

func (b *BitgetClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
