package bitrue

type InstrumentInfoResponse struct {
	Category       string         `json:"category"`
	NextPageCursor string         `json:"nextPageCursor"`
	List           []BitrueSymbol `json:"list"`
}

type BitrueSymbol struct {
	Symbol    string `json:"symbol"`
	Status    string `json:"status"`
	BaseCoin  string `json:"baseCoin"`
	QuoteCoin string `json:"quoteCoin"`
}

type WsTradeEvent struct {
	Id        int     `json:"-"`
	Price     float64 `json:""`
	Amount    float64 `json:""`
	Side      string  `json:""`
	Vol       float64 `json:""`
	Timestamp int64   `json:""`
}

type Ticker struct {
	// Ticker
	// {"tick": {"amount":1448711.193,"rose":0.0262,"close":3.435,"vol":429024.8,"high":3.435,"low":3.317,"open":3.347} }
	Amount float64 `json:"amount,omitempty"`
	Rose   float64 `json:"rose,omitempty"`
	Close  float64 `json:"close,omitempty"`
	Vol    float64 `json:"vol,omitempty"`
	High   float64 `json:"high,omitempty"`
	Low    float64 `json:"low,omitempty"`
	Open   float64 `json:"open,omitempty"`
}

type WsTradeTickerEvent struct {
	Ticker

	// Trade
	// {"tick": "data": [{"id":209312400,"price":0.6445,"amount":6.960,"side":"SELL","vol":10.8,"ts":1709423437882,"ds":"2024-03-03 07:50:37"}] }
	Data []WsTradeEvent `json:"data,omitempty"`
}
type WsTradeMessage struct {
	Channel   string             `json:"channel"`
	CbId      string             `json:"cb_id"`
	Timestamp int64              `json:"ts"`
	Tick      WsTradeTickerEvent `json:"tick"`
}

type TickerResponse struct {
	TickData  Ticker `json:"tick"`
	Channel   string `json:"channel"`
	Timestamp uint64 `json:"ts"`
}
