package bitmart

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type BitmartClient struct {
	name          string
	W             *sync.WaitGroup
	TickerTopic   *tickertopic.TickerTopic
	wsClient      internal.WebSocketClient
	wsEndpoint    string
	SymbolList    []model.Symbol
	lastTimestamp time.Time
	log           *slog.Logger

	pingInterval int
}

func NewBitmartClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BitmartClient, error) {
	wsEndpoint := "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"

	bitmart := BitmartClient{
		name:         "bitmart",
		log:          slog.Default().With(slog.String("datasource", "bitmart")),
		W:            w,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:   wsEndpoint,
		SymbolList:   symbolList.Crypto,
		pingInterval: 15,
	}
	bitmart.wsClient.SetMessageHandler(bitmart.onMessage)
	bitmart.wsClient.SetOnConnect(bitmart.onConnect)

	bitmart.wsClient.SetLogger(bitmart.log)
	bitmart.log.Debug("Created new datasource")
	return &bitmart, nil
}

func (b *BitmartClient) Connect() error {
	b.W.Add(1)

	b.wsClient.Start()

	b.setPing()
	b.setLastTickerWatcher()

	return nil
}

func (b *BitmartClient) onConnect() error {
	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}
func (b *BitmartClient) Close() error {
	b.wsClient.Close()
	b.W.Done()

	return nil
}

func (b *BitmartClient) onMessage(message internal.WsMessage) {
	if message.Type == websocket.TextMessage {
		msg := string(message.Message)
		if strings.Contains(msg, `"event":"subscribe"`) {
			return
		}

		if strings.Contains(msg, `"table":"spot/ticker"`) {
			tickers, err := b.parseTicker(message.Message)
			if err != nil {
				b.log.Error("Error parsing ticker",
					"error", err.Error())
				return
			}
			b.lastTimestamp = time.Now()

			for _, v := range tickers {
				b.TickerTopic.Send(v)
			}
		}

		// decompress
		/*decompressedData, err := internal.DecompressFlate(message.Message)
						if err != nil {
							b.log.Error("Error decompressing message", "error", err.Error())
							return nil
						}
						data := string(decompressedData)
						if strings.Contains(data, "_ticker") && strings.Contains(data, "tick") && !strings.Contains(data, "event_rep") {
							ticker, err := b.parseTicker([]byte(data))
							if err != nil {
								b.log.Error("Error parsing ticker",
				"ticker",ticker,"error", err.Error())
								return nil
							}
							b.lastTimestamp = time.Now()
		b.TickerTopic.Send(ticker)
						}*/
	}
}

func (b *BitmartClient) parseTicker(message []byte) ([]*model.Ticker, error) {
	var newTickerEvent wsTickerMessage
	err := sonic.Unmarshal(message, &newTickerEvent)
	if err != nil {
		b.log.Error(err.Error())
		return []*model.Ticker{}, err
	}

	tickers := []*model.Ticker{}
	for _, t := range newTickerEvent.Data {
		symbol := model.ParseSymbol(t.Symbol)
		newTicker, err := model.NewTicker(t.LastPrice,
			symbol,
			b.GetName(),
			time.UnixMilli(t.TimestampMs))
		if err != nil {
			b.log.Error("Error parsing ticker",
				"ticker", newTicker, "error", err.Error())
			continue
		}
		tickers = append(tickers, newTicker)
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
		b.wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	b.log.Debug("Subscribed ticker symbols")

	return nil
}

func (b *BitmartClient) GetName() string {
	return b.name
}

func (b *BitmartClient) setLastTickerWatcher() {
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

func (b *BitmartClient) setPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			b.wsClient.SendMessage(internal.WsMessage{Type: websocket.TextMessage, Message: []byte("ping")})
		}
	}()
}
