-by LightFTSO

Flow goes like this:

1. Configure and create data consumers (file, redis, mqtt, websocket, influxdb, questdb, etc)
2. Instantiate a (list of) data source(s) (could be crypto exchange, forex api, stocks api, or whatever)
3. Send parsed data to each of the consumers via a broadcast channel
4. Optionally generate and print statistics in a determined interval of time
5. ???
6. profit

Supported exchanges:

Crypto:
binance,binance.us,bitget,bitmart,bitrue,bitstamp,bybit,coinbase,cryptocom,digifinex,fmfw,gateio,
hitbtc,huobi,kraken,kucoin,lbank,mexc,okx,whitebit,toobit,

Stocks, Commodities, Forex:
tiingo,metalsdev

Other:


You can count the number of tickers per second enabling the file-output consumer, using /dev/stdout as the output file
or using the MQTT consumer, in another terminal connect to it using a client program and pipe the output to the program `pv`, e.g.:

`./ftso-data-sources | pv --line-mode --timer --rate > /dev/null`
Outputs:
`03:22 [ 496 /s]`
For more info on `pv`, visit [https://docs.oracle.com/cd/E86824_01/html/E54763/pv-1.html](Oracle's man pages)