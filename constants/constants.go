package constants

import (
	"strings"
)

type AssetName string
type AssetList []string

var BASES_CRYPTO = AssetList{"FLR", "SGB", "BTC", "XRP", "LTC", "XLM", "DOGE",
	"ADA", "ALGO", "ETH", "FIL", "ARB", "AVAX", "BNB", "MATIC", "SOL", "USDC", "USDT", "XDC",
	"TRX", "DOT", "SHIB", "UNI", "HBAR", "NEAR", "VET", "RNDR", "STRK", "AAVE", "QNT",
	"XTZ", "GALA", "ATOM", "ETC", "BEAM", "IMX", "STX", "APT", "OP", "ICP", "INJ", "TIA", "GRT", "SUI", "LDO"}

var BASES_FOREX = AssetList{
	"EUR",
	"JPY",
	"AUD",
}
var BASES_COMMODITIES = AssetList{
	"XAU",
	"XAG",
	"XPT",
}

var BASES_STOCKS = AssetList{}

const USD = "USD"

const USDT = "USDT"
const USDC = "USDC"
const DAI = "DAI"
const BUSD = "BUSD"

var USD_USDT_USDC_DAI = AssetList{USD, USDT, USDC, DAI}
var USD_USDT_USDC_DAI_BUSD = AssetList{USD, USDT, USDC, DAI, BUSD}
var USD_USDT_USDC = AssetList{USD, USDT, USDC}
var USDT_USDC_DAI = AssetList{USDT, USDC, DAI}
var USDT_USDC = AssetList{USDT, USDC}
var ALL_QUOTE_ASSETS = AssetList{USD, USDT, USDC, DAI}

const TS_FORMAT = "01/02/2006 03:04:05.000"

func IsStablecoin(asset string) bool {
	for _, a := range USD_USDT_USDC_DAI {
		if a == asset {
			return true
		}
	}
	return false
}

func IsValidQuote(asset string) bool {
	for _, a := range ALL_QUOTE_ASSETS {
		if strings.ToUpper(a) == asset {
			return true
		}
	}
	return false
}
