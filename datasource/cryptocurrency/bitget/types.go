package bitget

type WsTickerMessage struct {
	Action string `json:"action"`
	Arg    struct {
	} `json:"arg"`
	Data []struct {
		InstId    string `json:"instId"`
		LastPrice string `json:"lastPr"`
		Timestamp string `json:"ts"`
	} `json:"data"`
	Timestamp int64 `json:"ts"`
}
