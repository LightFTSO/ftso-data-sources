package metalsdev

var sampleLatest = []byte(`{
	"status": "success",
	"currency": "USD",
	"unit": "toz",
	"metals": {
		"gold": 2128.045,
		"silver": 23.67235,
		"platinum": 881.802,
		"palladium": 944.29,
		"lbma_gold_am": 2083.15,
		"lbma_gold_pm": 2098.05,
		"lbma_silver": 23.1,
		"lbma_platinum_am": 893,
		"lbma_platinum_pm": 889,
		"lbma_palladium_am": 964,
		"lbma_palladium_pm": 948,
		"mcx_gold": 2431.9513,
		"mcx_gold_am": 2407.3423,
		"mcx_gold_pm": 2416.3831,
		"mcx_silver": 27.5165,
		"mcx_silver_am": 26.9417,
		"mcx_silver_pm": 27.0231,
		"ibja_gold": 2423.4029,
		"copper": 0.2636,
		"aluminum": 0.069,
		"lead": 0.0636,
		"nickel": 0.548,
		"zinc": 0.0761,
		"lme_copper": 0.2653,
		"lme_aluminum": 0.0692,
		"lme_lead": 0.0635,
		"lme_nickel": 0.5544,
		"lme_zinc": 0.0758
	},
	"currencies": {
		"AED": 0.27226222,
		"AFN": 0.0136669,
		"ALL": 0.0105,
		"AMD": 0.00248533,
		"ANG": 0.558242,
		"AOA": 0.00118424,
		"ARS": 0.0012,
		"AUD": 0.65051,
		"AWG": 0.558659,
		"AZN": 0.5884,
		"BAM": 0.5551,
		"BBD": 0.5,
		"BDT": 0.0091,
		"BGN": 0.5551,
		"BHD": 2.6526,
		"BIF": 0.000349138,
		"BMD": 1,
		"BND": 0.744362,
		"BOB": 0.1447,
		"BRL": 0.2013,
		"BSD": 1,
		"BTC": 63259.45,
		"BTN": 0.012064,
		"BWP": 0.0732,
		"BYN": 0.306204,
		"BYR": 0.0000306204,
		"BZD": 0.494256,
		"CAD": 0.73553,
		"CDF": 0.000360939,
		"CHF": 1.13165,
		"CLP": 0.0010203,
		"CNH": 0.1387,
		"CNY": 0.13892,
		"COP": 0.00025346,
		"CRC": 0.001948,
		"CUC": 1,
		"CUP": 0.0417,
		"CVE": 0.00984376,
		"CZK": 0.042849,
		"DJF": 0.00562496,
		"DKK": 0.1456,
		"DOP": 0.016995,
		"DZD": 0.0074,
		"EEK": 0.0694,
		"EGP": 0.0324,
		"ERN": 0.0666667,
		"ETB": 0.0176,
		"EUR": 1.08563,
		"FJD": 0.442683,
		"FKP": 1.2705,
		"GBP": 1.27054,
		"GEL": 0.3769,
		"GGP": 1.26899,
		"GHS": 0.0785,
		"GIP": 1.2705,
		"GMD": 0.0147,
		"GNF": 0.00011631,
		"GTQ": 0.128,
		"GYD": 0.00478709,
		"HKD": 0.12781,
		"HNL": 0.0404,
		"HRK": 0.1441,
		"HTG": 0.00753115,
		"HUF": 0.00276,
		"IDR": 0.000063408,
		"ILS": 0.27826,
		"IMP": 1.26899,
		"INR": 0.012061,
		"IQD": 0.000763211,
		"IRR": 0.0000237695,
		"ISK": 0.007282,
		"JEP": 1.26899,
		"JMD": 0.006443,
		"JOD": 1.4103,
		"JPY": 0.006671,
		"KES": 0.007,
		"KGS": 0.0111819,
		"KHR": 0.000245871,
		"KMF": 0.00220639,
		"KPW": 0.00111112,
		"KRW": 0.0007494000000000001,
		"KWD": 3.2501,
		"KYD": 1.21759,
		"KZT": 0.0022,
		"LAK": 0.0000479848,
		"LBP": 0.000011,
		"LKR": 0.0032479999999999996,
		"LRD": 0.00520139,
		"LSL": 0.052674,
		"LTL": 0.3144,
		"LVL": 1.54476,
		"LYD": 0.20718,
		"MAD": 0.0996,
		"MDL": 0.056384,
		"MGA": 0.0002215,
		"MKD": 0.0176,
		"MMK": 0.000476749,
		"MNT": 0.0002959,
		"MOP": 0.1241,
		"MRU": 0.0251059,
		"MUR": 0.0218194,
		"MVR": 0.0649181,
		"MWK": 0.0005945,
		"MXN": 0.05903,
		"MYR": 0.2112,
		"MZN": 0.0156596,
		"NAD": 0.052674,
		"NGN": 0.0006405,
		"NIO": 0.0271,
		"NOK": 0.0945,
		"NPR": 0.007541,
		"NTD": 0.0337206,
		"NZD": 0.6086,
		"OMR": 2.5975,
		"PAB": 1,
		"PEN": 0.266941,
		"PGK": 0.2659,
		"PHP": 0.017865,
		"PKR": 0.00360713,
		"PLN": 0.2518,
		"PYG": 0.0001372,
		"QAR": 0.2742,
		"RON": 0.2184,
		"RSD": 0.0093,
		"RUB": 0.010952,
		"RWF": 0.000777056,
		"SAR": 0.2666,
		"SBD": 0.119942,
		"SCR": 0.0728,
		"SDG": 0.00166466,
		"SEK": 0.096346,
		"SGD": 0.7449,
		"SHP": 1.26899,
		"SKK": 0.036037,
		"SLL": 0.0000441035,
		"SOS": 0.00175438,
		"SPL": 6,
		"SRD": 0.0283331,
		"STN": 0.0441648,
		"SVC": 0.114286,
		"SYP": 0.0000769124,
		"SZL": 0.052674,
		"THB": 0.027933,
		"TJS": 0.0912593,
		"TMT": 0.286242,
		"TND": 0.3211,
		"TOP": 0.422446,
		"TRY": 0.03154,
		"TTD": 0.1476,
		"TVD": 0.650736,
		"TWD": 0.031668,
		"TZS": 0.000392,
		"UAH": 0.026,
		"UGX": 0.00025499999999999996,
		"USD": 1,
		"UYU": 0.0256,
		"UZS": 0.000080173,
		"VED": 0.0277441,
		"VEF": 2.77441e-7,
		"VND": 0.000040496999999999995,
		"VUV": 0.0082,
		"WST": 0.3677,
		"XAF": 0.001655,
		"XCD": 0.3704,
		"XDR": 1.32845,
		"XOF": 0.001655,
		"XPF": 0.00909625,
		"YER": 0.004,
		"ZAR": 0.052674,
		"ZMW": 0.0418,
		"ZWD": 0.00276319
	},
	"timestamps": {
		"metal": "2024-03-05T23:04:14.711Z",
		"currency": "2024-03-05T23:05:35.591Z"
	}
}
`)
