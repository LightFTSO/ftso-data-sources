package binance

import (
	"log/slog"
	"sync"

	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

func NewBinanceUSClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*BinanceClient, error) {
	wsEndpoint := "wss://stream.binance.us:9443/stream?streams="

	binance := BinanceClient{
		name:        "binance.us",
		log:         slog.Default().With(slog.String("datasource", "binance.us")),
		W:           w,
		TickerTopic: tickerTopic,
		wsClient:    *internal.NewWebSocketClient(wsEndpoint),
		wsEndpoint:  wsEndpoint,
		apiEndpoint: "https://api.binance.us",
		SymbolList:  symbolList.Crypto,
	}
	binance.wsClient.SetMessageHandler(binance.onMessage)
	binance.wsClient.SetOnConnect(binance.onConnect)
	binance.wsClient.SetOnConnect(binance.onConnect)
	binance.wsClient.SetLogger(binance.log)
	binance.log.Debug("Created new datasource")
	return &binance, nil
}
