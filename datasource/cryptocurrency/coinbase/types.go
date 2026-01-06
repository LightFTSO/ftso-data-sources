package coinbase

type CoinbaseSubscriptionSuccessMessage struct {
	Type string `json:"type"`

	Channels []struct {
		Name       string   `json:"ticker"`
		ProductIds []string `json:"product_ids"`
	}
}

type CoinbaseTicker struct {
	Type      string `json:"type"`
	ProductId string `json:"product_id"`
	LastPrice string `json:"price"`
	Timestamp string `json:"time"`
}
