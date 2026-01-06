package htx

type HTXTicker struct {
	Channel   string  `json:"ch"`
	Timestamp int64   `json:"ts"`
	Tick      HTXTick `json:"tick"`
}

type HTXTick struct {
	LastPrice float64 `json:"close"` // "close" is the last price
}
