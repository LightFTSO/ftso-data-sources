package digifinex

type MarketInfo struct {
	Data []DigifinexMarket `json:"data"`
}

type DigifinexMarket struct {
	Market string `json:"market"`
}

type wsTickerMessage struct {
	Method string       `json:"method"`
	Params []TickerData `json:"params"`
}

type TickerData struct {
	LastPrice string `json:"last"`
	Timestamp int64  `json:"timestamp"`
	Symbol    string `json:"symbol"`
}
