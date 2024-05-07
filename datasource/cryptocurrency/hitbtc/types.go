package hitbtc

type wsTickerMessage struct {
	Channel string                `json:"ch"`
	Data    map[string]tickerData `json:"data"`
}

type tickerData struct {
	Timestamp int64  `json:"t"` // Timestamp in milliseconds
	LastPrice string `json:"c"` // Last price
}
