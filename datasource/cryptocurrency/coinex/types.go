package coinex

type CoinexMarkets struct {
	Data []CoinexSymbol `json:"Data"`
}

type CoinexSymbol struct {
	Market string `json:"market"`
}

type WsTickerMessage struct {
	Method string `json:"method"`
	Data   struct {
		Market   string   `json:"market"`
		DealList []Ticker `json:"deal_list"`
	} `json:"data"`
}

type Ticker struct {
	Price     string `json:"price"`
	Timestamp int64  `json:"created_at"`
}
