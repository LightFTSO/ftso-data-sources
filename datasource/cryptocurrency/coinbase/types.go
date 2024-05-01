package coinbase

type CoinbaseTicker struct {
	LastPrice string `json:"price"`
	ProductId string `json:"product_id"`
	Timestamp string `json:"time"`
}

type CoinbaseSubscriptionSuccessMessage struct {
	Type string `json:"type"`

	Channels []struct {
		Name       string   `json:"ticker"`
		ProductIds []string `json:"product_ids"`
	}
}
