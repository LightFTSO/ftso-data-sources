package constants

import (
	"strings"
)

type AssetName string
type AssetList []string

var BASES_CRYPTO = AssetList{"flr", "sgb", "btc", "xrp", "ltc", "xlm", "doge",
	"ada", "algo", "eth", "fil", "arb", "avax", "bnb", "matic", "pol", "sol", "usdc", "usdt", "xdc",
	"trx", "dot", "shib", "uni", "hbar", "near", "vet", "rndr", "strk", "aave", "qnt",
	"xtz", "gala", "atom", "etc", "beam", "imx", "stx", "apt", "op", "icp", "inj", "tia", "grt", "sui", "ldo"}

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
const DAI = "dai"
const BUSD = "busd"

var USD_USDT_USDC_DAI = AssetList{USD, USDT, USDC, DAI}
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
