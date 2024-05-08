package binance

import (
	log "log/slog"
	"sync"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/symbols"
)

func NewBinanceUSClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.us:9443/stream?streams="

	binance := BinanceClient{
		name:        "binance.us",
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebsocketClient(wsEndpoint, true, nil),
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.binance.us",
		SymbolList:  symbolList.Crypto,
	}
	binance.wsClient.SetMessageHandler(binance.onMessage)
	log.Debug("Created new datasource", "datasource", binance.GetName())
	return &binance, nil
}
