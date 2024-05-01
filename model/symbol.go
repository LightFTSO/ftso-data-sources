package model

import (
	"strings"

	"roselabs.mx/ftso-data-sources/constants"
)

type Symbol struct {
	Base   string `mapstructure:",squash"`
	Quote  string
	Symbol string
}

func ParseSymbol(s string) Symbol {
	pair := cleanRemotePair(s)
	base := getBaseCurrency(pair)
	quote := strings.Replace(pair, base, "", 1)
	return Symbol{
		Base:   base,
		Quote:  quote,
		Symbol: base + "/" + quote,
	}
}

func cleanRemotePair(s string) string {
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ToUpper(s)
	return s
}

func getBaseCurrency(pair string) string {
	var baseCurrencies = constants.USDT_USDC_BUSD_DAI

	for _, baseCurrency := range baseCurrencies {
		if strings.HasPrefix(pair, strings.ToUpper(baseCurrency)) {
			return strings.ToUpper(baseCurrency)
		}
	}

	var quote = substr(pair, len(pair)-4, 6)

	if !strings.HasPrefix(quote, "U") && !strings.HasPrefix(quote, "B") {
		quote = "USD"
	}

	var base = strings.Replace(pair, quote, "", 1)

	if len(base) == 2 {
		quote = "USD"
		base = strings.Replace(pair, quote, "", 1)
	}

	return base
}

func substr(input string, start int, length int) string {
	asRunes := []rune(input)

	if start >= len(asRunes) {
		return ""
	}

	if start+length > len(asRunes) {
		length = len(asRunes) - start
	}

	return string(asRunes[start : start+length])
}
