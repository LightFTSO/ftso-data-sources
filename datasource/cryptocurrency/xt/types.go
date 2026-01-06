package xt

type WsTickerMessage struct {
	Data XtTickerData `json:"data"`
}

type XtTickerData struct {
	Symbol    string `json:"s"` // "btc_usdt"
	LastPrice string `json:"c"` // "30000.00"
	Timestamp int64  `json:"t"` // ms
}
