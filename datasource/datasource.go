package datasource

import (
	"fmt"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/binance"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bitfinex"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bitget"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bitmart"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bitrue"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bitstamp"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/bybit"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/coinbase"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/coinex"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/cryptocom"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/digifinex"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/fmfw"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/gateio"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/hitbtc"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/huobi"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/kraken"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/kucoin"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/lbank"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/mexc"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/okx"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/pionex"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/toobit"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/whitebit"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/xt"
	"roselabs.mx/ftso-data-sources/datasource/others/noisy"
	"roselabs.mx/ftso-data-sources/datasource/tradfi/metalsdev"
	"roselabs.mx/ftso-data-sources/datasource/tradfi/tiingo"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type FtsoDataSource interface {
	SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error
	Connect() error
	Close() error
	GetName() string
	IsRunning() bool
}

type DataSourceOptions struct {
	Source  string                 `mapstructure:"source"`
	Options map[string]interface{} `mapstructure:"options"`
}

func BuildDataSource(source DataSourceOptions, allSymbols symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (FtsoDataSource, error) {

	switch source.Source {
	case "binance":
		return binance.NewBinanceClient(source.Options, allSymbols, tickerTopic, w)
	case "binanceus":
		return binance.NewBinanceUSClient(source.Options, allSymbols, tickerTopic, w)
	case "bitfinex":
		return bitfinex.NewBitfinexClient(source.Options, allSymbols, tickerTopic, w)
	case "bitget":
		return bitget.NewBitgetClient(source.Options, allSymbols, tickerTopic, w)
	case "bitmart":
		return bitmart.NewBitmartClient(source.Options, allSymbols, tickerTopic, w)
	case "bitrue":
		return bitrue.NewBitrueClient(source.Options, allSymbols, tickerTopic, w)
	case "bitstamp":
		return bitstamp.NewBitstampClient(source.Options, allSymbols, tickerTopic, w)
	case "bybit":
		return bybit.NewBybitClient(source.Options, allSymbols, tickerTopic, w)
	case "coinbase":
		return coinbase.NewCoinbaseClient(source.Options, allSymbols, tickerTopic, w)
	case "coinex":
		return coinex.NewCoinexClient(source.Options, allSymbols, tickerTopic, w)
	case "cryptocom":
		return cryptocom.NewCryptoComClient(source.Options, allSymbols, tickerTopic, w)
	case "digifinex":
		return digifinex.NewDigifinexClient(source.Options, allSymbols, tickerTopic, w)
	case "fmfw":
		return fmfw.NewFmfwClient(source.Options, allSymbols, tickerTopic, w)
	case "gateio":
		return gateio.NewGateIoClient(source.Options, allSymbols, tickerTopic, w)
	case "hitbtc":
		return hitbtc.NewHitbtcClient(source.Options, allSymbols, tickerTopic, w)
	case "huobi":
		return huobi.NewHuobiClient(source.Options, allSymbols, tickerTopic, w)
	case "kraken":
		return kraken.NewKrakenClient(source.Options, allSymbols, tickerTopic, w)
	case "kucoin":
		return kucoin.NewKucoinClient(source.Options, allSymbols, tickerTopic, w)
	case "lbank":
		return lbank.NewLbankClient(source.Options, allSymbols, tickerTopic, w)
	case "mexc":
		return mexc.NewMexcClient(source.Options, allSymbols, tickerTopic, w)
	case "okx":
		return okx.NewOkxClient(source.Options, allSymbols, tickerTopic, w)
	case "pionex":
		return pionex.NewPionexClient(source.Options, allSymbols, tickerTopic, w)
	case "toobit":
		return toobit.NewToobitClient(source.Options, allSymbols, tickerTopic, w)
	case "whitebit":
		return whitebit.NewWhitebitClient(source.Options, allSymbols, tickerTopic, w)
	case "xt":
		return xt.NewXtClient(source.Options, allSymbols, tickerTopic, w)

	// non crypto
	case "tiingo":
		return tiingo.NewTiingoFxClient(source.Options, allSymbols, tickerTopic, w)
	case "metalsdev":
		var options = new(metalsdev.MetalsDevOptions)
		mapstructure.Decode(source.Options, options)
		return metalsdev.NewMetalsDevClient(options, allSymbols, tickerTopic, w)

	// sample datasource
	case "noisy":
		var options = new(noisy.NoisySourceOptions)
		mapstructure.Decode(source.Options, options)
		return noisy.NewNoisySource(options, allSymbols, tickerTopic, w)

	default:
		return nil, fmt.Errorf("source '%s' doesn't exist", source.Source)
	}

}
