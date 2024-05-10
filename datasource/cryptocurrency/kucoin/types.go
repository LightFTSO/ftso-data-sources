package kucoin

type BulletPublicEndpointResponse struct {
	Code string `json:"code"`
	Data struct {
		Token           string           `json:"token"`
		InstanceServers []InstanceServer `json:"instanceServers"`
	} `json:"data"`
}

type InstanceServer struct {
	Endpoint       string `json:"endpoint"`
	Encrypt        bool   `json:"encrypt"`
	Protocol       string `json:"protocol"`
	PingIntervalMs int    `json:"pingInterval"`
	PingTimeout    int    `json:"pingTimeout"`

	Token string `json:"-"`
}

type KucoinSymbol struct {
	Symbol        string `json:"symbol"`
	BaseCurrency  string `json:"baseCurrency"`
	QuoteCurrency string `json:"quoteCurrency"`
}

type WsTickerMessage struct {
	Type    string `json:"type"`
	Topic   string `json:"topic"`
	Subject string `json:"subject"`
	Data    struct {
		Price string `json:"price"`
		Time  int64  `json:"Time"`
	} `json:"data"`
}
