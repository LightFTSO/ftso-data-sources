package bitrue

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

type BitrueClient struct {
	name        string
	W           *sync.WaitGroup
	TradeTopic  *broadcast.Broadcaster
	TickerTopic *broadcast.Broadcaster
	wsClient    internal.WebsocketClient
	wsEndpoint  string
	apiEndpoint string
	SymbolList  []model.Symbol

	pingInterval int
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewBitrueClient(options interface{}, symbolList symbols.AllSymbols, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BitrueClient, error) {
	log.Info("Created new datasource", "datasource", "bitrue")
	wsEndpoint := "wss://ws.bitrue.com/kline-api/ws"

	bitrue := BitrueClient{
		name:         "bitrue",
		W:            w,
		TradeTopic:   tradeTopic,
		TickerTopic:  tickerTopic,
		wsClient:     *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:   wsEndpoint,
		apiEndpoint:  "https://api.bitrue.com",
		SymbolList:   symbolList.Crypto,
		pingInterval: 20,
	}
	bitrue.wsClient.SetMessageHandler(bitrue.onMessage)

	return &bitrue, nil
}

func (b *BitrueClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to bitrue datasource")

	b.ctx, b.cancel = context.WithCancel(context.Background())

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}

	go b.wsClient.Listen()

	return nil
}

func (b *BitrueClient) Reconnect() error {
	log.Info("Reconnecting to bitrue datasource")

	_, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info("Reconnected to bitrue datasource")
	err = b.SubscribeTrades()
	if err != nil {
		log.Error("Error subscribing to trades", "datasource", b.GetName())
		return err
	}
	go b.wsClient.Listen()
	return nil
}
func (b *BitrueClient) Close() error {
	b.wsClient.Close()
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *BitrueClient) onMessage(message internal.WsMessage) error {
	if message.Err != nil {
		log.Error("Error reading websocket data, reconnecting in 5 seconds",
			"datasource", b.GetName(), "error", message.Err)
		time.Sleep(1 * time.Second)
		b.Reconnect()
	}

	if message.Type == websocket.TextMessage {
		fmt.Println(string(message.Message))

		if strings.Contains(string(message.Message), "ping") {
			log.Debug("Pong received", "datasource", "kraken")
			b.wsClient.SendMessageJSON(strings.ReplaceAll(string(message.Message), "ping", "pong"))
			return nil
		}
	}

	if message.Type == websocket.BinaryMessage {
		// decompress
		data, err := b.decompressGzip(message.Message)
		if err != nil {
			log.Error("Error parsing binary message", "datasource", b.GetName(), "error", err.Error())
		}
		if strings.Contains(string(data), "_trade_ticker") && strings.Contains(string(data), "tick") && !strings.Contains(string(data), "event_rep") {
			trades, err := b.parseTrade(data)
			if err != nil {
				log.Error("Error parsing trade", "datasource", b.GetName(), "error", err.Error())

			}
			for _, v := range trades {
				b.TradeTopic.Send(v)

			}
		}
	}

	return nil
}

func (b *BitrueClient) parseTrade(message []byte) ([]*model.Trade, error) {
	var newEvent WsTradeMessage
	err := json.Unmarshal(message, &newEvent)
	if err != nil {
		log.Error(err.Error(), "datasource", b.GetName())
		return []*model.Trade{}, err
	}

	pair := strings.ReplaceAll(newEvent.Channel, "market_", "")
	pair = strings.ReplaceAll(pair, "_trade_ticker", "")
	symbol := model.ParseSymbol(pair)
	trades := []*model.Trade{}
	for _, trade := range newEvent.Tick.Data {
		trades = append(trades, &model.Trade{
			Base:      symbol.Base,
			Quote:     symbol.Quote,
			Symbol:    symbol.Symbol,
			Price:     strconv.FormatFloat(trade.Price, 'f', 9, 64),
			Size:      strconv.FormatFloat(trade.Vol, 'f', 9, 64),
			Source:    b.GetName(),
			Timestamp: time.UnixMilli(trade.Timestamp),
		})
	}

	return trades, nil
}

func (b *BitrueClient) getAvailableSymbols() ([]BitrueSymbol, error) {
	reqUrl := b.apiEndpoint + "/v5/market/instruments-info?category=spot"

	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	type instrumentInfoResponse struct {
		InstrumentsInfo *InstrumentInfoResponse `json:"result"`
	}
	var exchangeInfo = new(instrumentInfoResponse)
	err = json.Unmarshal(data, exchangeInfo)
	if err != nil {
		return nil, err
	}

	return exchangeInfo.InstrumentsInfo.List, nil

}

func (b *BitrueClient) SubscribeTrades() error {
	for _, v := range b.SymbolList {
		cb_id := fmt.Sprintf("%s%s", strings.ToLower(v.Base), strings.ToLower(v.Quote))

		subMessage := map[string]interface{}{
			"event": "sub",
			"params": map[string]interface{}{
				"channel": fmt.Sprintf("market_%s_trade_ticker", cb_id),
				"cb_id":   cb_id,
			},
		}
		b.wsClient.SendMessageJSON(subMessage)
		log.Debug("Subscribed trade symbol", "datasource", b.GetName(), "symbols", v.Symbol)
	}

	return nil
}

func (b *BitrueClient) SubscribeTickers() error {
	return nil
}

func (b *BitrueClient) GetName() string {
	return b.name
}

func (b *BitrueClient) decompressGzip(compressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(compressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		log.Error("Error decompressing message", "datasource", b.GetName(), "error", err.Error())
		return []byte{}, err
	}
	data, _ := io.ReadAll(r)
	r.Close()

	return data, nil
}

func (b *BitrueClient) SetPing() {
	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	go func() {
		defer ticker.Stop() // Ensure the ticker is stopped when this goroutine ends
		for {
			select {
			case <-ticker.C: // Wait until the ticker sends a signal
				if err := b.wsClient.Connection.WriteMessage(websocket.PingMessage, []byte(`{"op":"ping"}`)); err != nil {
					log.Warn("Failed to send ping", "error", err, "datasource", b.GetName())
				}
			case <-b.ctx.Done():
				return
			}
		}
	}()
}
