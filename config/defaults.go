package config

import (
	"github.com/spf13/viper"
	"roselabs.mx/ftso-data-sources/constants"
)

func setDefaults() {
	viper.SetDefault("env", "development")
	viper.SetDefault("log_level", "info")

	viper.SetDefault("message_buffer_size", 0)

	viper.SetDefault("assets.crypto", constants.BASES_CRYPTO)
	viper.SetDefault("assets.commodities", constants.BASES_COMMODITIES)
	viper.SetDefault("assets.forex", constants.BASES_FOREX)
	viper.SetDefault("assets.stocks", constants.BASES_STOCKS)

	viper.SetDefault("stats.enabled", "true")
	viper.SetDefault("stats.interval", "60s")
	viper.SetDefault("stats.num_threads", 1)

	viper.SetDefault("datasources", []string{"noisy"})

	viper.SetDefault("file_consumer.enabled", false)
	viper.SetDefault("file_consumer.filename", "")

	viper.SetDefault("mosquitto.enabled", false)
	viper.SetDefault("mosquitto.num_threads", 1)
	viper.SetDefault("mosquitto.use_sbe_encoding", true)
	viper.SetDefault("mosquitto.qos_level", 0)

	viper.SetDefault("redis.enabled", false)
	viper.SetDefault("redis.client_options.initaddress", []string{"127.0.0.1:6379"})
	viper.SetDefault("redis.client_options.username", "")
	viper.SetDefault("redis.client_options.password", "")
	viper.SetDefault("redis.num_threads", 1)
	viper.SetDefault("redis.include_stdout", false)
	viper.SetDefault("redis.ts.retention", "24h")
	viper.SetDefault("redis.ts.chunksize", 4096)

	viper.SetDefault("websocket_server.enabled", false)
	viper.SetDefault("websocket_server.use_sbe_encoding", false)
	viper.SetDefault("websocket_server.host", "127.0.0.1")
	viper.SetDefault("websocket_server.port", 3000)
	viper.SetDefault("websocket_server.endpoints.tickers", "/tickers")
	viper.SetDefault("websocket_server.endpoints.volumes", "/volumes")

	viper.SetDefault("questdb.enabled", false)
	viper.SetDefault("questdb.flush_interval", "10s")
	viper.SetDefault("questdb.client_options.address", "127.0.0.0.1:9000")
	viper.SetDefault("questdb.client_options.schema", "http")
}
