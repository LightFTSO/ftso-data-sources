package kucoin

type BulletResponse struct {
	Code string `json:"code"`
	Data struct {
		Token           string `json:"token"`
		InstanceServers []struct {
			Endpoint     string `json:"endpoint"`
			PingInterval int64  `json:"pingInterval"`
		} `json:"instanceServers"`
	} `json:"data"`
}

type WsTickerEvent struct {
	Topic string     `json:"topic"`
	Data  KucoinData `json:"data"`
}

type KucoinData struct {
	Price string `json:"price"`
	Time  int64  `json:"time"`
}
