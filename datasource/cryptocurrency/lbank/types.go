package lbank

type wsTickerMessage struct {
	Type      string     `json:"type"`
	Pair      string     `json:"pair"`
	Timestamp string     `json:"TS"`
	Ticker    tickerData `json:"tick"`
}

type tickerData struct {
	LastPrice float64 `json:"latest"`
}
