package okx

type OkxTicker struct {
	Data []struct {
		InstId string `json:"instId"`
		Last   string `json:"last"`  // Spot Last Price
		Idxpx  string `json:"idxPx"` // Index Price (if available)
		Ts     string `json:"ts"`
	} `json:"data"`
}
