package factory

import (
	"fmt"
	"sync"
	"time"

	"roselabs.mx/ftso-data-sources/datasource"
	crypto "roselabs.mx/ftso-data-sources/datasource/binance"
	"roselabs.mx/ftso-data-sources/datasource/noisy"
	"roselabs.mx/ftso-data-sources/model"
)

func BuilDataSource(kind string, subscribedSymbols []model.Symbol, tradeChan *chan model.Trade, w *sync.WaitGroup) (datasource.FtsoDataSource, error) {

	switch kind {
	case "binance":
		return crypto.NewBinanceWebsocketClient(subscribedSymbols, tradeChan, w), nil
	case "noisy":
		return noisy.NewNoisySource("noisy", 1*time.Second, tradeChan, w), nil

	default:
		return nil, fmt.Errorf("source type %s doesn't exist", kind)
	}

}
