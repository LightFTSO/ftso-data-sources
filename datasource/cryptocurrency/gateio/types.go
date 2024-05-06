package gateio

type GateIoInstrument struct {
	Id    string `json:"id"`
	Base  string `json:"base"`
	Quote string `json:"quote"`
}

type WsTickerMessage struct {
	Time    int64  `json:"time"`
	TimeMs  int64  `json:"time_ms"`
	Channel string `json:"channel"`
	Event   string `json:"event"`
	Result  struct {
		CurrencyPair string `json:"currency_pair"`
		Last         string `json:"last"`
	} `json:"result"`
}
