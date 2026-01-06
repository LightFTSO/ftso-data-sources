package bitstamp

type wsTickerMessage struct {
	Event   string       `json:"event"`
	Channel string       `json:"channel"`
	Data    bitstampData `json:"data"`
}

type bitstampData struct {
	LastPrice      string `json:"price_str"` // Bitstamp sends "price_str" usually, or "price"
	TimestampMicro string `json:"microtimestamp"`
}
