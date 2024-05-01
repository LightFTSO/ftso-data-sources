package okx

type OkxTicker struct {
	Arg struct {
		Channel string `json:"channel"`
		InstId  string `json:"instId"`
	} `json:"arg"`

	Data []struct {
		InstId  string `json:"instId"`
		Idxpx   string `json:"idxPx"`
		Open24h string `json:"open24h"`
		High24h string `json:"high24h"`
		Low24h  string `json:"low24h"`
		Sodutc0 string `json:"sodUtc0"`
		Sodutc8 string `json:"sodUtc8"`
		Ts      string `json:"ts"`
	} `json:"data"`
}
