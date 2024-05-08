package bitstamp

type wsTickerMessage struct {
	Channel string     `json:"channel"`
	Event   string     `json:"event"`
	Data    tickerData `json:"data"`
}

type tickerData struct {
	Id             int    `json:"id"`
	Timestamp      string `json:"timestamp"`      // Timestamp in seconds
	TimestampMicro string `json:"microtimestamp"` // Timestamp in microseconds
	LastPrice      string `json:"price_str"`      // Last price
}
