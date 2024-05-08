package whitebit

type WhitebitMarketPair struct {
	Name string `json:"name"`
}

type WsTickerMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}
