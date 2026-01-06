package lbank

type wsTickerMessage struct {
	Type      string     `json:"type"`
	Pair      string     `json:"pair"`
	Timestamp string     `json:"TS"` // e.g. "2024-05-20T12:00:00.000"
	Ticker    tickerData `json:"tick"`
}

type tickerData struct {
	LastPrice float64 `json:"latest"`
}
