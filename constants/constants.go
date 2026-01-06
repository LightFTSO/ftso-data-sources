package constants

import (
	"strings"
)

type AssetName string
type AssetList []string

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
const USDS = "USDS"
const BUSD = "BUSD"

var USD_USDT_USDC_DAI_USDS = AssetList{USD, USDT, USDC, DAI, USDS}
var USD_USDT_USDC_DAI__USDS_BUSD = AssetList{USD, USDT, USDC, DAI, BUSD, USDS}
var USD_USDT_USDC = AssetList{USD, USDT, USDC}
var USDT_USDC_DAI_USDS = AssetList{USDT, USDC, DAI, USDS}
var USDT_USDC = AssetList{USDT, USDC}
var ALL_QUOTE_ASSETS = AssetList{USD, USDT, USDC, USDS}

const TS_FORMAT = "01/02/2006 03:04:05.000"

func IsStablecoin(asset string) bool {
	for _, a := range USD_USDT_USDC_DAI_USDS {
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
