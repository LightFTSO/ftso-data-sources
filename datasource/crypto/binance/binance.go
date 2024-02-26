package crypto

import (
	"errors"
	"fmt"
	log "log/slog"
	"net/http"
	"sync"

	"roselabs.mx/ftso-data-sources/model"
	internal "roselabs.mx/ftso-data-sources/websocket"
)

func NewBinanceWebsocketClient(tradeChan *chan model.Trade, w *sync.WaitGroup) *BinanceWebsocketClient {
	endpoint := "wss://stream.binance.com:9443/stream?streams="

	binance := BinanceWebsocketClient{
		Name:      "binance",
		W:         w,
		TradeChan: tradeChan,
		wsClient:  *internal.NewWebsocketClient(endpoint, true),
	}

	return &binance
}

type BinanceWebsocketClient struct {
	Name      string `json:"name"`
	W         *sync.WaitGroup
	TradeChan *chan model.Trade

	wsClient internal.WebsocketClient `json:"-"`
}

type BinanceData struct {
	s string  `json:"s"`
	c float64 `json:"c"`
	p float64 `json:"p"`
	q float64 `json:"q"`
	t int64   `json:"T"`
}

func (b *BinanceWebsocketClient) Connect() error {
	response, err := b.wsClient.Connect(http.Header{})
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("%+v", response))

	return nil
}

func (b *BinanceWebsocketClient) StartTrades() error {

	return nil
}

func (b *BinanceWebsocketClient) SubscribeTrades(baseList []string, quoteList []string) ([]string, error) {

	err := errors.New("an error occurred")
	return []string{}, err
}

func (b *BinanceWebsocketClient) Close() error {
	b.W.Done()

	return nil
}
