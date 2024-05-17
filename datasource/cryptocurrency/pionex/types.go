package pionex

type SymbolsResponse struct {
	Result bool `json:"result"`
	Data   struct {
		Symbols []struct {
			BaseCurrency  string `json:"baseCurrency"`
			QuoteCurrency string `json:"quoteCurrency"`
		} `json:"symbols"`
	} `json:"data"`
}

type wsTickerMessage struct {
	Topic  string       `json:"topic"`
	Symbol string       `json:"symbol"`
	Data   []tickerData `json:"data"`
}

type tickerData struct {
	Symbol    string `json:"symbol"`
	Timestamp int64  `json:"timestamp"` // Timestamp in milliseconds
	LastPrice string `json:"price"`     // Last price
}
