package bitmart

type WsTickerMessage struct {
	Table string        `json:"table"`
	Data  []BitmartData `json:"data"`
}

type BitmartData struct {
	Symbol      string `json:"symbol"` // e.g. BTC_USDT
	LastPrice   string `json:"last_price"`
	QuoteVolume string `json:"quote_volume"`
	BaseVolume  string `json:"base_volume"`
	TimestampMs int64  `json:"ms_t"` // Server timestamp
}
