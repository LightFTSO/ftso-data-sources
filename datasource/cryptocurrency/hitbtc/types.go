package hitbtc

type WsTickerMessage struct {
	Data map[string]HitbtcTickerData `json:"data"`
}

type HitbtcTickerData struct {
	LastPrice string `json:"c"` // c = Close price
	Timestamp int64  `json:"t"` // t = Timestamp (ms)
}
