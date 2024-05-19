package bitfinex

type SubscribeMessage struct {
	Event     string `json:"event"`
	ChannelId int    `json:"chanId"`
	Pair      string `json:"pair"`
}

type TickerEvent []interface{}

type ChannelSymbolMap map[int]string
