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
