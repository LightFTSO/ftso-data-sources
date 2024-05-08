package bitmart

type wsTickerMessage struct {
	Table string       `json:"table"`
	Data  []tickerData `json:"data"`
}

type tickerData struct {
	LastPrice   string `json:"last_price"`
	TimestampMs int64  `json:"ms_t"`
	TimestampS  int64  `json:"s_t"`
	Symbol      string `json:"symbol"`
}
