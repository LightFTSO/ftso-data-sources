package fmfw

type WsTickerMessage struct {
	Channel string                    `json:"ch"`
	Data    map[string]FmfwTickerData `json:"data"` // Key is Symbol
}

type FmfwTickerData struct {
	LastPrice string `json:"c"` // 'c' = close price
	Timestamp int64  `json:"t"` // 't' = timestamp (ms)
}
