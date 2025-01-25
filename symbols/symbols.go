package symbols

import (
	"slices"
	"strings"

	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
)

func cartesianProduct(params ...[]interface{}) chan []interface{} {
	// create channel
	c := make(chan []interface{})
	if len(params) == 0 {
		close(c)
		return c // Return a safe value for nil/empty params.
	}
	go func() {
		iterate(c, params[0], []interface{}{}, params[1:]...)
		close(c)
	}()
	return c
}

func iterate(channel chan []interface{}, topLevel, result []interface{}, needUnpacking ...[]interface{}) {
	if len(needUnpacking) == 0 {
		for _, p := range topLevel {
			channel <- append(append([]interface{}{}, result...), p)
		}
		return
	}
	for _, p := range topLevel {
		iterate(channel, needUnpacking[0], append(result, p), needUnpacking[1:]...)
	}
}

func createSymbolList(bases, quotes []string) (model.SymbolList, error) {

	a := make([]interface{}, len(bases))
	for i, v := range bases {
		a[i] = v
	}

	b := make([]interface{}, len(quotes))
	for i, v := range quotes {
		b[i] = v
	}

	c := cartesianProduct(a, b)

	// receive products through channel
	symbols := model.SymbolList{}
	for product := range c {
		symbols = append(symbols, model.Symbol{
			Base:  strings.ToUpper(product[0].(string)),
			Quote: strings.ToUpper(product[1].(string)),
		})
	}
	return symbols, nil
}

type AllSymbols struct {
	Crypto      model.SymbolList `mapstructure:"crypto"`
	Forex       model.SymbolList `mapstructure:"forex"`
	Commodities model.SymbolList `mapstructure:"commodities"`
	Stocks      model.SymbolList `mapstructure:"stocks"`
}

func (s *AllSymbols) Flatten() model.SymbolList {
	return slices.Concat(s.Crypto, s.Forex, s.Commodities, s.Stocks)
}

func GetAllSymbols(crypto []string, commodities []string, forex []string, stocks []string) AllSymbols {
	cryptoSymbols, err := createSymbolList(crypto[:], constants.ALL_QUOTE_ASSETS[:])
	if err != nil {
		panic(err)
	}
	forexSymbols, err := createSymbolList(forex[:], []string{constants.USD}[:])
	if err != nil {
		panic(err)
	}
	commoditySymbols, err := createSymbolList(commodities[:], []string{constants.USD}[:])
	if err != nil {
		panic(err)
	}
	stockSymbols, err := createSymbolList(stocks[:], []string{constants.USD}[:])
	if err != nil {
		panic(err)
	}

	return AllSymbols{
		Crypto:      cryptoSymbols,
		Forex:       forexSymbols,
		Commodities: commoditySymbols,
		Stocks:      stockSymbols,
	}
}
