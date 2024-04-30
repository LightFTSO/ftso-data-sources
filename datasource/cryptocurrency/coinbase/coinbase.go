package coinbase

import (
	"sync"

	log "log/slog"

	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
)

type CoinbaseClient struct {
	Name        string                   `json:"name"`
	W           *sync.WaitGroup          `json:"-"`
	TradeChan   chan model.Trade         `json:"-"`
	wsClient    internal.WebsocketClient `json:"-"`
	wsEndpoint  string                   `json:"-"`
	apiEndpoint string                   `json:"-"`
	SymbolList  []model.Symbol           `json:"symbolList"`

	wsMessageChan chan internal.WsMessage
}

func NewCoinbaseClient(symbolList []model.Symbol, tradeChan chan model.Trade, w *sync.WaitGroup) *CoinbaseClient {
	log.Info("Created new datasource", "datasource", "coinbase")
	wsEndpoint := "wss://ws-feed.exchange.coinbase.com"

	coinbase := CoinbaseClient{
		Name:          "coinbase",
		W:             w,
		TradeChan:     tradeChan,
		wsClient:      *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:    wsEndpoint,
		apiEndpoint:   "https://api.binance.com",
		SymbolList:    symbolList,
		wsMessageChan: make(chan internal.WsMessage),
	}

	return &coinbase
}
