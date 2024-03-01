-by LightFTSO

Data flow goes like this:

1. Configure and create data consumers (file, redis, mqtt, websocket, influxdb, questdb, etc)
2. Instantiate a (list of) data source(s) (could be crypto exchange, forex api, stocks api, or whatever)
2. Send parsed trade and/or ticker data to each of the consumers via a broadcast channel
3. Optionally generate and print statistics in a determined interval of time
4. ?
5. profit