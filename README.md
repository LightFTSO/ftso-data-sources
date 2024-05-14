Created with ❤️ LightFTSO

# Introduction
Flow goes like this:
1. Configure and create data consumers (file, redis, mqtt, websocket, questdb, etc)
2. Instantiate data source(s) (could be crypto exchange, forex api, stocks api, or whatever)
3. Broadcast ticker data to each of the consumers
4. Consume the data, that part depends on you
5. Optionally generate and print statistics every determined interval of time
6. ???
7. profit

# Supported data sources:
### Crypto:
    binance,binance.us,bitget,bitmart,bitrue,bitstamp,bybit,coinbase,cryptocom,digifinex,fmfw,gateio,
    hitbtc,huobi,kraken,kucoin,lbank,mexc,okx,toobit,whitebit,xt

### Stocks, Commodities, Forex:
    tiingo,metalsdev

### Other:
    dummy data source for testing

# Running the program

1. Clone the repository
`git clone https://github.com/LightFTSO/ftso-data-sources.git`
2. cd into it
`cd ftso-data-sources`
3a. Run locally with `make run` (you need `make` and `go` installed)
```bash
# you need go 1.22+ installed, see https://go.dev/doc/install
# if you dont have make installed
sudo apt install build-essential # if using ubuntu, refer to your distro for more info on installing make and gcc
make run
```
3b. Run in docker:
`docker compose up -d ftso-data-sources`

Supported go version: 1.22+

# Configuration 
Modify the following sample configuration, by default, the program will look for a file called `config.yaml` in it's root folder or you can specify the file with the `-config <file>` flag
```yaml
# set to anything other than 'production' to not panic when this configuration wont produce any data
env: production

# set log level
log_level: info # debug, info, warn, error

datasources:
  - source: binance
  - source: binance.us
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
  num_threads: 2
  interval: 60s

# see https://mqtt.org/
mqtt:
  enabled: false
  url: "tcp://localhost:1883"
  num_threads: 4
  use_sbe_encoding: true
  qos_level: 1

# see https://questdb.io/
questdb:
  enabled: false
  flush_interval: 10s
  individual_feed_table: true
  client_options:
    address: localhost:9000
    
# See https://redis.io/docs/latest/develop/data-types/timeseries/
redis_ts:
  enabled: false
  include_stdout: False
  num_threads: 12
  ts:
    retention: 1h
    chunksize: 2048
  client_options:
    initaddress: # list of redis instance or cluster nodes
      - "127.0.0.1:6379" 
    username:
    password:

# append output directly to a file
file_output:
  enabled: false
  filename: tickers.txt

websocket_server:
  enabled: false
  host: 127.0.0.1
  port: 9999
  use_sbe_encoding: false
  endpoints:
    tickers: /tickers
    volumes:
    
assets:
  forex:
  #  - eur
  #  - jpy
  #  - aud
  commodities:
  crypto:
    - flr
    - sgb
    - xrp
    - ltc
    - xlm
    - doge
    - ada
    - algo
    - btc
    - eth
    - fil
    - arb
    - avax
    - bnb
    - matic
    - sol
    - usdc
    - usdt
    - xdc

# change the capacity of the internal message buffer
# change it only if you experience performance issues
# (expected to happen only with thousands of messages per second)
message_buffer_size: 65536
```
You can count the number of tickers per second enabling the file-output consumer, using /dev/stdout as the output file
or using the MQTT consumer, in another terminal connect to it using a client program and pipe the output to the program `pv`, e.g.:

`./ftso-data-sources | pv --line-mode --timer --rate > /dev/null`
Outputs:
`03:22 [ 496 /s]`
For more info on `pv`, visit [https://docs.oracle.com/cd/E86824_01/html/E54763/pv-1.html](Oracle's man pages)