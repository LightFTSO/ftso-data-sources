package kraken

type ApiError struct {
	Category string `json:"category"`
	Message  string `json:"message"`
}

type ApiResponse struct {
	Error  []string    `json:"error"`
	Result interface{} `json:"result"`
}

type ApiAssetPairResponse struct {
	Error  []string                 `json:"error"`
	Result map[string]AssetPairInfo `json:"result"`
}

type KrakenAsset string

func (a *KrakenAsset) GetStdName() KrakenAsset {
	name := KrakenAssetMapping[*a]
	if name == "" {
		return *a
	}
	return name
}

type AssetPairInfo struct {
	Altname         string      `json:"altname"`
	AssetClassBase  string      `json:"aclass_base"`
	Base            KrakenAsset `json:"base"`
	AssetClassQuote string      `json:"aclass_quote"`
	Quote           KrakenAsset `json:"quote"`
	WsName          string      `json:"wsname"`
}

type WsTickerEvent []string

type WsTickerMessage []interface{}

// https://support.kraken.com/hc/en-us/articles/360001185506-How-to-interpret-asset-codes
var KrakenAssetMapping = map[KrakenAsset]KrakenAsset{
	"XBT":    "BTC",
	"XXBT":   "BTC",
	"XDG":    "DOGE",
	"XDAO":   "DAO",
	"XETC":   "ETC",
	"XETH":   "ETH",
	"XICN":   "ICN",
	"XLTC":   "LTC",
	"XMLN":   "MLN",
	"XNMC":   "NMC",
	"XREP":   "REP",
	"XREPV2": "REPV2",
	"XXLM":   "XLM",
	"XXMR":   "XMR",
	"XXRP":   "XRP",
	"XXVN":   "XVN",
	"XZEC":   "ZEC",
	"XXDG":   "DOGE",

	"ZAUD": "AUD",
	"ZCAD": "CAD",
	"ZCHF": "CHF",
	"ZEUR": "EUR",
	"ZGBP": "GBP",
	"ZJPY": "JPY",
	"ZUSD": "USD",
}

type KrakenSnapshotUpdate struct {
	Channel string         `json:"channel"`
	Type    string         `json:"type"`
	Data    []KrakenTicker `json:"data"`
}

type KrakenTicker struct {
	Ask       float64 `json:"ask"`        // Best ask price.
	AskQty    float64 `json:"ask_qty"`    // Best ask quantity.
	Bid       float64 `json:"bid"`        // Best bid price.
	BidQty    float64 `json:"bid_qty"`    // Best bid quantity.
	Change    float64 `json:"change"`     // 24-hour price change (in quote currency).
	ChangePct float64 `json:"change_pct"` // 24-hour price change (in percentage points).
	High      float64 `json:"high"`       // 24-hour highest trade price.
	Last      float64 `json:"last"`       // Last traded price.
	Low       float64 `json:"low"`        // 24-hour lowest trade price.
	Symbol    string  `json:"symbol"`     // Example: "BTC/USD" The symbol of the currency pair.
	Volume    float64 `json:"volume"`     // 24-hour traded volume (in base currency terms).
	Vwap      float64 `json:"vwap"`       // 24-hour volume weighted average price.
}
