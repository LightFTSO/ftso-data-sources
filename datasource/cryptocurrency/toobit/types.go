package toobit

type WsTickerMessage struct {
	Topic  string `json:"topic"`
	Symbol string `json:"symbol"`
	Params struct {
		RealtimeInterval string `json:"realtimeinterval"`
		Binary           bool   `json:"binary"`
	} `json:"params"`
	Data []struct {
		Timestamp int64  `json:"t"`
		Symbol    string `json:"s"`
		Close     string `json:"c"`
	} `json:"data"`
}
