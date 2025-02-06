package binance

import (
	"log/slog"
	"sync"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/symbols"
)

func NewBinanceUSClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.us:9443/stream?streams="

	binance := BinanceClient{
		name:             "binanceus",
		log:              slog.Default().With(slog.String("datasource", "binanceus")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.binance.us",
		SymbolList:       symbolList.Crypto,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	binance.symbolChunks = binance.SymbolList.ChunkSymbols(1024)
	binance.log.Debug("Created new datasource")
	return &binance, nil
}
