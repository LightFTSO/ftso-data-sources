package metalsdev

type LatestEndpointResponse struct {
	Status     string             `json:"status"`
	Currency   string             `json:"currency"`
	Unit       string             `json:"unit"`
	Metals     map[string]float64 `json:"metals"`
	Currencies map[string]float64 `json:"currencies"`
	Timestamps struct {
		Metal    string `json:"metal"`
		Currency string `json:"currency"`
	}
}

var metalsDevCommodityMap = map[string]string{
	"XAU": "gold",
	"XAG": "silver",
	"XPT": "platinum",
	"XCU": "copper",
	"XAL": "aluminum",
	"XPB": "lead",
	"XNI": "nickel",
	"XZN": "zinc",
}

var metalsDevCurrencyMap = map[string]string{}
