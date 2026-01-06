package bitrue

type TickerResponse struct {
	Channel   string     `json:"channel"` // e.g. market_btcusdt_ticker
	Timestamp int64      `json:"ts"`
	TickData  BitrueTick `json:"tick"`
}

type BitrueTick struct {
	Close float64 `json:"close"`
	Vol   float64 `json:"vol"`
}
