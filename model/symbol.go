package model

import (
	"strings"

	"roselabs.mx/ftso-data-sources/constants"
)

type Symbol struct {
	Base  string `mapstructure:",squash"`
	Quote string
}

func (s *Symbol) GetSymbol() string {
	return s.Base + "/" + s.Quote
}

func ParseSymbol(s string) Symbol {
	pair := cleanRemotePair(s)
	base := getBaseCurrency(pair)
	quote := strings.Replace(pair, base, "", 1)

	return Symbol{
		Base:  base,
		Quote: quote,
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
	for _, stablecoin := range constants.USDT_USDC_DAI {
		upperSc := strings.ToUpper(stablecoin)
		if strings.HasPrefix(pair, upperSc) {
			return upperSc
		}
	}

	upperDAI := strings.ToUpper(constants.DAI)
	if strings.HasSuffix(pair, upperDAI) {
		return strings.Replace(pair, upperDAI, "", 1)
	}

	var quote = substr(pair, len(pair)-4, 6)

	if !strings.HasPrefix(quote, "U") /*&& !strings.HasPrefix(quote, "B") */ {
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

type SymbolList []Symbol

func (s SymbolList) ChunkSymbols(chunkSize int) []SymbolList {
	var chunks []SymbolList
	for i := 0; i < len(s); i += chunkSize {
		end := i + chunkSize
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[i:end])
	}
	return chunks

}
