package cryptocom

type PublicHeartbeat struct {
	Id     uint64 `json:"id"`
	Method string `json:"method"`
	Code   int    `json:"code"`
}

type WsTickerMessage struct {
	Id     int64  `json:"id"`
	Method string `json:"method"`
	Code   int64  `json:"code"`
	Result struct {
		IntrumentName string       `json:"instrument_name"`
		Subscription  string       `json:"subscription"`
		Channel       string       `json:"channel"`
		Data          []TickerData `json:"data"`
	}
}

type TickerData struct {
	LastPrice      string `json:"a"`
	InstrumentName string `json:"i"`
	Timestamp      int64  `json:"t"`
}
