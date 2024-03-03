package constants

type AssetName string
type AssetList []string

var BASES_CRYPTO = AssetList{"gala"}

// AssetList{"flr", "sgb", "btc", "xrp", "ltc", "xlm", "doge",
//	"ada", "algo", "eth", "fil", "arb", "avax", "bnb", "matic", "sol", "usdc", "usdt", "xdc",
//	"trx", "dot", "shib", "uni", "hbar", "near", "vet", "rndr", "strk", "aave", "qnt",
//	"xtz", "gala", "atom", "etc", "beam", "imx", "stx", "apt", "op", "icp", "inj", "tia", "grt", "sui", "ldo"}

var BASES_FOREX = AssetList{
	"eur",
	"jpy",
	"aud",
}
var BASES_COMMODITIES = AssetList{
	"xau",
	"xag",
	"xpt",
}

var BASES_STOCKS = AssetList{}

const USD = "usd"
const USDT = "usdt"
const USDC = "usdc"
const BUSD = "busd"

var USD_USDT_USDC_BUSD = AssetList{USD, USDT, USDC, BUSD}
var USD_USDT_USDC = AssetList{USD, USDT, USDC}
var USDT_USDC_BUSD = AssetList{USDT, USDC, BUSD}
var USDT_USDC = AssetList{USDT, USDC}

const TS_FORMAT = "01/02/2006 03:04:05.000"
