package factory

import (
	"fmt"
	"sync"
	"time"

	"roselabs.mx/ftso-data-sources/datasource"
	crypto "roselabs.mx/ftso-data-sources/datasource/crypto/binance"
	"roselabs.mx/ftso-data-sources/model"
)

func BuilDataSource(kind string, tradeChan *chan model.Trade, w *sync.WaitGroup) (datasource.FtsoDataSource, error) {

	switch kind {
	case "binance":
		return crypto.NewBinanceWebsocketClient(tradeChan, w), nil
	case "noisy":
		return datasource.NewNoisySource("noisy", 1*time.Second, tradeChan, w), nil

	default:
		return nil, fmt.Errorf("source type %s doesn't exist", kind)
	}

}
