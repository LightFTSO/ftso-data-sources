package gateio

type WsTickerMessage struct {
	TimeMs int64      `json:"time_ms"`
	Result GateIoData `json:"result"`
}

type GateIoData struct {
	CurrencyPair string `json:"currency_pair"` // e.g. BTC_USDT
	Last         string `json:"last"`
}

type GateIoInstrument struct {
	Base  string `json:"base"`
	Quote string `json:"quote"`
}
