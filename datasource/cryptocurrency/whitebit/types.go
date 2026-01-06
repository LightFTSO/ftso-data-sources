package whitebit

type WsTickerMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type WhitebitMarketPair struct {
	Name string `json:"name"` // e.g. "BTC_USDT"
}
