package datasource

import (
	"fmt"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/datasource/binance"
	"roselabs.mx/ftso-data-sources/datasource/bitrue"
	"roselabs.mx/ftso-data-sources/datasource/bybit"
	"roselabs.mx/ftso-data-sources/datasource/kraken"
	"roselabs.mx/ftso-data-sources/datasource/noisy"
	"roselabs.mx/ftso-data-sources/symbols"
)

type FtsoDataSource interface {
	SubscribeTrades() error
	SubscribeTickers() error
	Connect() error
	Reconnect() error
	Close() error
	GetName() string
}

type DataSourceList []FtsoDataSource

type DataSourceOptions struct {
	Source  string                 `mapstructure:"source"`
	Options map[string]interface{} `mapstructure:",remain"`
}

func BuilDataSource(source DataSourceOptions, allSymbols symbols.AllSymbols, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (FtsoDataSource, error) {

	switch source.Source {
	case "binance":
		return binance.NewBinanceClient(source.Options, allSymbols, tradeTopic, tickerTopic, w)
	case "bitrue":
		return bitrue.NewBitrueClient(source.Options, allSymbols, tradeTopic, tickerTopic, w)
	case "bybit":
		return bybit.NewBybitClient(source.Options, allSymbols, tradeTopic, tickerTopic, w)
	case "kraken":
		return kraken.NewKrakenClient(source.Options, allSymbols, tradeTopic, tickerTopic, w)
	case "noisy":
		var options = new(noisy.NoisySourceOptions)
		mapstructure.Decode(source.Options, options)
		return noisy.NewNoisySource(options, tradeTopic, tickerTopic, w)

	default:
		return nil, fmt.Errorf("source %s doesn't exist", source)
	}

}
