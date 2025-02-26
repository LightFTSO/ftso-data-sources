package config

import (
	"github.com/spf13/viper"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

func setDefaults() {
	viper.SetDefault("env", "development")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("message_buffer_size", 0)
	viper.SetDefault("port", 9999)

	viper.SetDefault("assets.crypto", constants.BASES_CRYPTO)
	viper.SetDefault("assets.commodities", constants.AssetList{})
	viper.SetDefault("assets.forex", constants.AssetList{})
	viper.SetDefault("assets.stocks", constants.AssetList{})

	viper.SetDefault("stats.enabled", true)
	viper.SetDefault("stats.interval", "60s")

	viper.SetDefault("datasources", []string{"noisy"})

	viper.SetDefault("file_output.enabled", false)
	viper.SetDefault("file_output.filename", "")

	viper.SetDefault("mqtt.enabled", false)
	viper.SetDefault("mqtt.qos_level", 0)

	viper.SetDefault("redis_ts.enabled", false)
	viper.SetDefault("redis_ts.client_options.initaddress", []string{"127.0.0.1:6379"})
	viper.SetDefault("redis_ts.client_options.username", "")
	viper.SetDefault("redis_ts.client_options.password", "")
	viper.SetDefault("redis_ts.ts.retention", "24h")
	viper.SetDefault("redis_ts.ts.chunksize", 2048)
	viper.SetDefault("redis_ts.ts.maxmemory", "1gb")

	viper.SetDefault("websocket_server.enabled", false)
	viper.SetDefault("websocket_server.ticker_endpoint", "/tickers")
	viper.SetDefault("websocket_server.flush_interval", "500ms")

	viper.SetDefault("questdb.enabled", false)
	viper.SetDefault("questdb.flush_interval", "10s")
	viper.SetDefault("questdb.client_options.address", "127.0.0.0.1:9000")
	viper.SetDefault("questdb.client_options.schema", "http")
	viper.SetDefault("questdb.individual_feed_table", true)

	viper.SetDefault("ticker_transformations", tickertopic.TransformationOptions{})
}
