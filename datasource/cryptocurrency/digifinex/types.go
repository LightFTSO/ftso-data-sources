package digifinex

type MarketInfo struct {
	Data []DigifinexMarket `json:"data"`
}

type DigifinexMarket struct {
	Market string `json:"market"`
}

type WsTickerMessage struct {
	Method string          `json:"method"`
	Params []DigifinexData `json:"params"`
}

type DigifinexData struct {
	Symbol    string `json:"symbol"`    // e.g. BTC_USDT
	LastPrice string `json:"last"`      // Digifinex usually sends float, but string is safer
	Timestamp int64  `json:"timestamp"` // ms?
}
