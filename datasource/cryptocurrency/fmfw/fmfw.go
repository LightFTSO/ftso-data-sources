package fmfw

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

type FmfwClient struct {
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

func NewFmfwClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*FmfwClient, error) {
	wsEndpoint := "wss://api.fmfw.io/api/3/ws/public"

	fmfw := FmfwClient{
		name:         "fmfw",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	fmfw.wsClient.SetMessageHandler(fmfw.onMessage)

	log.Debug("Created new datasource", "datasource", fmfw.GetName())
	return &fmfw, nil
}

func (b *FmfwClient) Connect() error {
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

func (b *FmfwClient) Reconnect() error {
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
func (b *FmfwClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *FmfwClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		if strings.Contains(string(message.Message), "ticker/price/1s") && strings.Contains(string(message.Message), "data") {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(),
					"error", err.Error())
				return nil
			}

			for _, v := range tickers {
				fmt.Println(v)
				b.TickerTopic.Send(v)
			}
		}
	}

	return nil
}

func (b *FmfwClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Ticker{}, err
	}

	keys := make([]string, 0, len(newTickerEvent.Data))
	for k := range newTickerEvent.Data {
		keys = append(keys, k)
	}

	tickers := []*model.Ticker{}
	for _, key := range keys {
		tickData := newTickerEvent.Data[key]
		symbol := model.ParseSymbol(key)
		newTicker, err := model.NewTicker(tickData.LastPrice,
			symbol,
			b.GetName(),
			time.UnixMilli(tickData.Timestamp))
		if err != nil {
			log.Error("Error parsing ticker", "datasource", b.GetName(),
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
	}

	return tickers, nil
}

func (b *FmfwClient) SubscribeTickers() error {
	// batch subscriptions in packets
	chunksize := len(b.SymbolList)
	for i := 0; i < len(b.SymbolList); i += chunksize {
		subMessage := map[string]interface{}{
			"ch":     "ticker/price/1s/batch",
			"method": "subscribe",
			"id":     time.Now().UnixMicro(),
			"params": map[string]interface{}{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(b.SymbolList) {
				continue
			}
			v := b.SymbolList[i+j]
			s = append(s, fmt.Sprintf("%s%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["params"] = map[string]interface{}{
			"symbols": s,
		}
		//fmt.Println(subMessage)

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Debug("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *FmfwClient) GetName() string {
	return b.name
}

func (b *FmfwClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
