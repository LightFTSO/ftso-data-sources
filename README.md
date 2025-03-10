Created with ❤️ LightFTSO

# Introduction
Flow goes like this:
1. Configure and create data consumers (file, redis, mqtt, websocket, questdb, etc)
2. Instantiate data source(s) (could be crypto exchange, forex api, stocks api, or whatever)
3. Broadcast ticker data to each of the consumers
4. Consume the data, that part depends on you
5. Optionally generate and print statistics every determined interval of time
6. Manage observed assets via JSON-RPC methods
7. ???
8. profit

# Supported data sources:
### Crypto:
    binance,binance.us,bitfinex,bitget,bitmart,bitrue,bitstamp,bybit,coinex,coinbase,cryptocom,digifinex,fmfw,gateio,
    hitbtc,huobi,kraken,kucoin,lbank,mexc,okx,pionex,toobit,whitebit,xt

### Stocks, Commodities, Forex:
    tiingo,metalsdev

### Other:
    dummy data source for testing

# Running the program

1. Clone the repository
`git clone https://github.com/LightFTSO/ftso-data-sources.git`
2. cd into it
`cd ftso-data-sources`
3. (optionally clone submodules for protobuf codegen)
`git submodule update --init --recursive`
4. Create the config.yaml (see the section below)
`touch config.yaml`
5. Run it 
Locally with `make run`:
You need go 1.22+ installed, see https://go.dev/doc/install
```bash
# if you dont have make installed
sudo apt install build-essential # if using ubuntu, refer to your distro for more info on installing make and gcc
make run
```
Run in docker:
`docker compose up -d ftso-data-sources`

Supported go version: 1.22+

# Protobuf Definitions and Codegen

See https://github.com/RoseLabsMx/ftso-data-sources-proto-files

# Configuration 
Modify the following sample configuration, by default, the program will look for a file called `config.yaml` in it's root folder or you can specify the file with the `-config <file>` flag

If using the WebSocket consumer on Docker, change the host from `127.0.0.1` to `0.0.0.0` so that the container is capable of listenning from external connections (e.g. from the host or from other containers)

```yaml
# set to anything other than 'production' to not panic when this configuration wont produce any data
env: production

# set log level
log_level: info # debug, info, warn, error

# port (for RPC and Websocket consumer)
port: 9999

datasources:
  - source: binance
  - source: binanceus
  - source: bitget
  - source: bitmart
  - source: bitrue
  - source: bitstamp
  - source: bybit
  - source: coinbase
  - source: cryptocom
  - source: digifinex
  - source: fmfw
  - source: gateio
  - source: hitbtc
  - source: huobi
  - source: kucoin
  - source: kraken
  - source: lbank
  - source: mexc
  - source: okx
  - source: toobit
  - source: whitebit
  - source: xt

  - source: tiingo
    api_token: <your-api-token>
  - source: metalsdev
    api_token: <your-api-token>
    interval: 90s

  # used for testing, creates one ticker with a random price for a random asset in the assets list
  - source: noisy
    name: noisy1
    interval: 1s

# prints the number of tickers per second every [interval] seconds
stats:
  enabled: true
  interval: 60s

# see https://mqtt.org/
mqtt:
  enabled: false
  url: "tcp://localhost:1883"
  qos_level: 1

# see https://questdb.io/
questdb:
  enabled: false
  flush_interval: 10s
  individual_feed_table: false
  client_options:
    address: localhost:9000
    
# See https://redis.io/docs/latest/develop/data-types/timeseries/
redis_ts:
  enabled: false
  include_stdout: False
  ts:
    retention: 1h
    chunksize: 2048
    maxmemory: 4gb
  client_options:
    initaddress:
      - "127.0.0.1:6379" 
    username:
    password:

# append output directly to a file
file_output:
  enabled: false
  filename: tickers.txt

websocket_server:
  enabled: false
  ticker_endpoint: /tickers
    
assets:
  forex:
  #  - eur
  #  - jpy
  #  - aud
  commodities:
  crypto:
    - ADA
    - ALGO
    - ARB
    - AVAX
    - BNB
    - BTC
    - DOGE
    - ETH
    - FIL
    - FLR
    - LTC
    - MATIC
    - SGB
    - SOL
    - USDC
    - USDT
    - XDC
    - XLM
    - XRP

# Ticker transformations change the data inside the generated tickers.
# Transformations affect every ticker from every exchange
# TODO: Add conditional ticker transformations
# So far, there are three types of ticker transformations:
# 'use_system_timestamp','rename_asset' and 'rename_quote_asset'
# 
# All transformations follow the same structure: type, enabled, and its options
ticker_transformations:
  - type: use_system_timestamp
	enabled: true
  # example: rename FTM to S:
  - type: rename_asset
    enabled: true
	from: FTM
	to: S
  # example rename USDT quote asset (the part after the /) to UST
  - type: rename_quote_asset
    enabled: true
	from: USDT
	to: UST

# change the capacity of the internal message buffer
# change it only if you experience performance issues
# (expected to happen only with many thousands (10,000+) of messages per second)
message_buffer_size: 65536
```

# JSON-RPC Functionality
The [JSON-RPC](https://pkg.go.dev/net/rpc/jsonrpc "JSON-RPC") endpoint is by default located at `http://localhost:{config.port}/rpc`. The default port is **9999**.

### Add new sssets
**POST http://127.0.0.1:9999/rpc**
```
{
	"method": "RPCManager.AddAsset",
	"params": [
		{
			"Assets": [
				{
					"AssetName": "ETH",
					"Category": "crypto"
				},
					{
					"AssetName": "BTC",
					"Category": "crypto"
				}
			]
		}
	],
	"id": 1
}
```
Response:
```
{
	"id": 1,
	"result": {
		"Message": "Assets {[{ETH crypto} {BTC crypto}]} added successfully"
	},
	"error": null
}
```

### Remove assets
**POST http://127.0.0.1:9999/rpc**
```
{
	"method": "RPCManager.RemoveAsset",
	"params": [
		{
			"Assets": [
				{
					"AssetName": "ETH",
					"Category": "crypto"
				},
					{
					"AssetName": "BTC",
					"Category": "crypto"
				}
			]
		}
	],
	"id": 1
}
```
Response:
```
{
	"id": 1,
	"result": {
		"Message": "Asset {[{ETH crypto} {BTC crypto}]} removed successfully"
	},
	"error": null
}
```

### Rename asset (e.g. MATIC to POL)
**POST http://127.0.0.1:9999/rpc**
```
{
	"method": "RPCManager.RenameAsset",
	"params": [
		{
			"AssetName": "MATIC",
			"NewName": "POL",
			"Category": "crypto"
		}
	],
	"id": 1
}
```
Response:
```
{
	"id": 1,
	"result": {
		"Message": "Asset MATIC (crypto) renamed to POL successfully"
	},
	"error": null
}
```

### Get asset list
**POST http://127.0.0.1:9999/rpc**
```
{
	"method": "RPCManager.GetAssets",
	"params": [],
	"id": 1
}
```
Response:
```
{
	"id": 1,
	"result": {
		"Assets": {
			"crypto": [
				"BTC",
				"ETH"
			],
			"commodities": [],
			"forex": [],
			"stocks": []
		}
	},
	"error": null
}
```

### Shutdown the program
**POST http://127.0.0.1:9999/rpc**
```
{
	"method": "RPCManager.Shutdown",
	"params": [],
	"id": 1
}
```
Response:
```
{
	"id": 1,
	"result": {
		"Assets": {
			"crypto": [
				"BTC",
				"ETH"
			],
			"commodities": [],
			"forex": [],
			"stocks": []
		}
	},
	"error": null
}
```

# Counting ticker rate
You can count the number of tickers per second enabling the file-output consumer, using /dev/stdout as the output file
or using the MQTT consumer, in another terminal connect to it using a client program and pipe the output to the program `pv`, e.g.:

`./ftso-data-sources | pv --line-mode --timer --rate > /dev/null`
Outputs:
`03:22 [ 496 /s]`
For more info on `pv`, visit [https://docs.oracle.com/cd/E86824_01/html/E54763/pv-1.html](Oracle's man pages)

# Contributing

Contributions are welcome! Please feel free to submit a PR.
