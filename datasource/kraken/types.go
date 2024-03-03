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

type WsTradeEvent []string

type WsTradeMessage []interface{}

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
