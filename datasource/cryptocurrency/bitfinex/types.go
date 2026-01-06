package bitfinex

type SubscribeMessage struct {
	Event     string `json:"event"`
	Channel   string `json:"channel"`
	ChannelId int    `json:"chanId"`
	Symbol    string `json:"symbol"`
	Pair      string `json:"pair"`
}

type TickerEvent []interface{}
