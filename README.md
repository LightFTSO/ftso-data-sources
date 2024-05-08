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
binance,binance.us,bitmart,bitrue,bitstamp,bybit,coinbase,cryptocom,gateio,hitbtc,huobi,kraken,lbank,okx

Stocks, Commodities, Forex:
tiingo,metalsdev

Other: