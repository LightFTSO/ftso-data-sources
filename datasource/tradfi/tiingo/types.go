package tiingo

type WsMessage struct {
	MessageType string `json:"messageType"`

	Data interface{} `json:"data"`
}

type WsInfoMessage struct {
	Data struct {
		SubscriptionId int      `json:"subscriptionId,omitempty"`
		Tickers        []string `json:"tickers,omitempty"`
		ThresholdLevel string   `json:"thresholdLevel,omitempty"`
	}
}

type WsFxEvent struct {
	Service string        `json:"string"`
	Data    []interface{} `json:"data"`
}
