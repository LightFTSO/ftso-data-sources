package config

import "github.com/spf13/viper"

func setDefaults() {
	viper.SetDefault("env", "development")

	viper.SetDefault("enabled_streams.trades", false)
	viper.SetDefault("enabled_streams.tickers", false)

	viper.SetDefault("message_buffer_size", 0)

	viper.SetDefault("stats.enabled", "1h")
	viper.SetDefault("stats.interval", "1h")
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
	viper.SetDefault("websocket_server.endpoints.trades", "/trades")
	viper.SetDefault("websocket_server.endpoints.tickers", "/tickers")
	viper.SetDefault("websocket_server.endpoints.volumes", "/volumes")
}
