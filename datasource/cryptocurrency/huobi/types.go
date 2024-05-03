package huobi

/*
	{
	  "ch": "market.ltcusdt.ticker",
	  "ts": 1714708576097,
	  "tick": {
	    "open": 79.82,
	    "high": 81.26,
	    "low": 78.61,
	    "close": 80.66,
	    "amount": 224688.3354257035,
	    "vol": 1.7980681626152623e7,
	    "count": 40082,
	    "bid": 80.65,
	    "bidSize": 12.3447,
	    "ask": 80.66,
	    "askSize": 4.0655,
	    "lastPrice": 80.66,
	    "lastSize": 4.2127
	  }
	}
*/
type HuobiTicker struct {
	Channel   string `json:"ch"`
	Timestamp int64  `json:"ts"`
	Tick      struct {
		//Open      float64 `json:"open"`
		//High      float64 `json:"high"`
		//Low       float64 `json:"low"`
		//Close     float64 `json:"close"`
		//Amount    float64 `json:"amount"`
		//Vol       float64 `json:"vol"`
		//Count     float64 `json:"count"`
		//Bid       float64 `json:"bid"`
		//BidSize   float64 `json:"bidSize"`
		//Ask       float64 `json:"ask"`
		//AskSize   float64 `json:"askSize"`
		LastPrice float64 `json:"lastPrice"`
		//LastSize  float64 `json:"lastSize"`
	}
}
