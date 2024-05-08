package bitmart

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

type BitmartClient struct {
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

func NewBitmartClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitmartClient, error) {
	wsEndpoint := "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"

	bitmart := BitmartClient{
		name:         "bitmart",
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	bitmart.wsClient.SetMessageHandler(bitmart.onMessage)

	log.Info("Created new datasource", "datasource", bitmart.GetName())
	return &bitmart, nil
}

func (b *BitmartClient) Connect() error {
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

func (b *BitmartClient) Reconnect() error {
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
func (b *BitmartClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BitmartClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket message",
			"datasource", b.GetName(), "error", message.Err)

		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, `"event":"subscribe"`) {
			return nil
		}

		if strings.Contains(msg, `"table":"spot/ticker"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil
			}

			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}
		}

		// decompress
		/*compressedData, err := internal.DecompressFlate(message.Message)
		if err != nil {
			log.Error("Error decompressing message", "datasource", b.GetName(), "error", err.Error())
			return nil
		}
		data := string(compressedData)
		fmt.Println(compressedData)
		if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
			ticker, err := b.parseTicker([]byte(data))
			if err != nil {
				log.Error("Error parsing ticker", "datasource", b.GetName(), "error", err.Error())
				return nil
			}
			b.TickerTopic.Send(ticker)
		}*/
	}

	return nil
}

func (b *BitmartClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := json.Unmarshal(message, &newTickerEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		ticker := model.Ticker{
			Base:      symbol.Base,
			Quote:     symbol.Quote,
			Symbol:    symbol.Symbol,
			LastPrice: t.LastPrice,
			Source:    b.GetName(),
			Timestamp: time.UnixMilli(t.TimestampMs),
		}
		tickers = append(tickers, &ticker)
	}

	return tickers, nil
}

func (b *BitmartClient) SubscribeTickers() error {
	// batch subscriptions in packets of 10
	chunksize := 10
	for i := 0; i < len(b.SymbolList); i += chunksize {
		subMessage := map[string]interface{}{
			"op":   "subscribe",
			"args": []string{},
		}
		s := []string{}
		for j := range chunksize {
			if i+j >= len(b.SymbolList) {
				continue
			}
			v := b.SymbolList[i+j]
			s = append(s, fmt.Sprintf("spot/ticker:%s_%s", strings.ToUpper(v.Base), strings.ToUpper(v.Quote)))
		}
		subMessage["args"] = s

		// sleep a bit to avoid rate limits
		time.Sleep(10 * time.Millisecond)
		b.wsClient.SendMessageJSON(subMessage)
	}

	log.Info("Subscribed ticker symbols", "datasource", b.GetName())

	return nil
}

func (b *BitmartClient) GetName() string {
	return b.name
}

func (b *BitmartClient) SetPing() {
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
