package xt

type MarketInfo struct {
	Data []DigifinexMarket `json:"data"`
}

type DigifinexMarket struct {
	Market string `json:"market"`
}

type WsTickerMessage struct {
	Topic string     `json:"topic"`
	Event string     `json:"event"`
	Data  TickerData `json:"data"`
}

type TickerData struct {
	Symbol    string `json:"s"`
	LastPrice string `json:"c"`
	Timestamp int64  `json:"t"`
}
