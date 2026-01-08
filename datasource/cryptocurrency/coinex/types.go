package coinex

type CoinexMarkets struct {
	Data []CoinexSymbol `json:"Data"`
}

type CoinexSymbol struct {
	Market string `json:"market"`
}

type Ticker struct {
	Price     string `json:"price"`
	Timestamp int64  `json:"created_at"`
}

type WsTickerMessage struct {
	Method string       `json:"method"`
	Data   CoinexParams `json:"data"`
}

type CoinexParams struct {
	Market   string       `json:"market"`
	DealList []CoinexDeal `json:"deal_list"`
}

type CoinexDeal struct {
	Price     string `json:"price"`
	Timestamp int64  `json:"created_at"` // Coinex V2 uses "created_at" (ms)
}
