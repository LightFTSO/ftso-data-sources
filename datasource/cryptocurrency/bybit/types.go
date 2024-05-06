package bybit

type InstrumentInfoResponse struct {
	Category       string        `json:"category"`
	NextPageCursor string        `json:"nextPageCursor"`
	List           []BybitSymbol `json:"list"`
}

type BybitSymbol struct {
	Symbol    string `json:"symbol"`
	Status    string `json:"status"`
	BaseCoin  string `json:"baseCoin"`
	QuoteCoin string `json:"quoteCoin"`
}

type WsTradeEvent struct {
	TradeTime int64  `json:"T"`
	Symbol    string `json:"s"`
	Side      string `json:"S"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Size      string `json:"v"`
}

type WsTradeMessage struct {
	Topic string         `json:"topic"`
	Time  int64          `json:"ts"`
	Type  string         `json:"type"`
	Data  []WsTradeEvent `json:"data"`
}

type WsTickerMessage struct {
	Topic string `json:"topic"`
	Time  int64  `json:"ts"`
	Type  string `json:"type"`
	Cs    uint   `json:"cs"`
	Data  struct {
		Symbol        string `json:"symbol"`
		LastPrice     string `json:"lastPrice"`
		HighPrice24h  string `json:"highPrice24h"`
		LowPrice24h   string `json:"lowPrice24h"`
		PrevPrice24h  string `json:"prevPrice24h"`
		Volume24h     string `json:"volume24h"`
		Turnover24h   string `json:"turnover24h"`
		Price24hPcnt  string `json:"price24hPcnt"`
		UsdIndexPrice string `json:"usdIndexPrice"`
	} `json:"data"`
}
