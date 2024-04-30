package binance

// WsTradeEvent define websocket trade event
type WsTradeEvent struct {
	Event    string `json:"e"`
	Time     int64  `json:"E"`
	Symbol   string `json:"s"`
	TradeID  int64  `json:"t"`
	Price    string `json:"p"`
	Quantity string `json:"q"`
	//BuyerOrderID  int64  `json:"b"`
	//SellerOrderID int64  `json:"a"`
	TradeTime    int64 `json:"T"`
	IsBuyerMaker bool  `json:"m"`
	Placeholder  bool  `json:"M"` // add this field to avoid case insensitive unmarshaling
}

type WsCombinedTradeEvent struct {
	Stream string       `json:"stream"`
	Data   WsTradeEvent `json:"data"`
}

type WsTickerMessage struct {
	Stream string        `json:"stream"`
	Data   WsTickerEvent `json:"data"`
}

// WsMarketStatEvent define websocket market statistics event
type WsTickerEvent struct {
	Event              string `json:"e"`
	Time               int64  `json:"E"`
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	WeightedAvgPrice   string `json:"w"`
	PrevClosePrice     string `json:"x"`
	LastPrice          string `json:"c"`
	CloseQty           string `json:"Q"`
	BidPrice           string `json:"b"`
	BidQty             string `json:"B"`
	AskPrice           string `json:"a"`
	AskQty             string `json:"A"`
	OpenPrice          string `json:"o"`
	HighPrice          string `json:"h"`
	LowPrice           string `json:"l"`
	BaseVolume         string `json:"v"`
	QuoteVolume        string `json:"q"`
	OpenTime           int64  `json:"O"`
	CloseTime          int64  `json:"C"`
	FirstID            int64  `json:"F"`
	LastID             int64  `json:"L"`
	Count              int64  `json:"n"`
}

type BinanceSymbol struct {
	Symbol     string `json:"symbol"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
}

type BinanceExchangeInfoResponse struct {
	Symbols []BinanceSymbol `json:"symbols"`
}
