package mexc

type MarketInfo struct {
	Data []DigifinexMarket `json:"data"`
}

type DigifinexMarket struct {
	Market string `json:"market"`
}

type WsTickerMessage struct {
	Channel   string     `json:"c"`
	Data      TickerData `json:"d"`
	Timestamp int64      `json:"t"`
	Symbol    string     `json:"s"`
}

type TickerData struct {
	Symbol    string `json:"s"`
	LastPrice string `json:"p"`
}
