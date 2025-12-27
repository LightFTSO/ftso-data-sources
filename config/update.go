package config

import (
	"log/slog"

	"github.com/spf13/viper"
)

func UpdateConfig(newConfig ConfigOptions, saveCurrentConfig bool) {
	// save the configuration object before changes are applied, for backup purposes
	if saveCurrentConfig {
		SaveConfig()
	}

	viper.Set("env", newConfig.Env)
	viper.Set("log_level", newConfig.LogLevel)
	viper.Set("message_buffer_size", newConfig.MessageBufferSize)
	viper.Set("port", newConfig.Port)

	viper.Set("assets.crypto", newConfig.Assets.Crypto)
	viper.Set("assets.commodities", newConfig.Assets.Commodities)
	viper.Set("assets.forex", newConfig.Assets.Forex)
	viper.Set("assets.stocks", newConfig.Assets.Stocks)

	viper.Set("stats.enabled", newConfig.Stats.Enabled)
	viper.Set("stats.interval", newConfig.Stats.Interval)

	viper.Set("datasources", newConfig.Datasources)

	viper.Set("file_output.enabled", newConfig.FileConsumerOptions.Enabled)
	viper.Set("file_output.filename", newConfig.FileConsumerOptions.OutputFilename)

	viper.Set("mqtt.enabled", newConfig.MQTTConsumerOptions.Enabled)
	viper.Set("mqtt.qos_level", newConfig.MQTTConsumerOptions.QOSLevel)

	viper.Set("redis_ts.enabled", newConfig.RedisOptions.Enabled)
	viper.Set("redis_ts.client_options.initaddress", newConfig.RedisOptions.ClientOptions.InitAddress)
	viper.Set("redis_ts.client_options.username", newConfig.RedisOptions.ClientOptions.Username)
	viper.Set("redis_ts.client_options.password", newConfig.RedisOptions.ClientOptions.Password)
	viper.Set("redis_ts.ts.retention", newConfig.RedisOptions.TsOptions.Retention)
	viper.Set("redis_ts.ts.chunksize", newConfig.RedisOptions.TsOptions.ChunkSize)
	viper.Set("redis_ts.ts.maxmemory", newConfig.RedisOptions.TsOptions.MaxMemory)

	viper.Set("websocket_server.enabled", newConfig.WebsocketConsumerOptions.Enabled)
	viper.Set("websocket_server.ticker_endpoint", newConfig.WebsocketConsumerOptions.TickersEndpoint)
	viper.Set("websocket_server.flush_interval", newConfig.WebsocketConsumerOptions.FlushInterval)
	viper.Set("websocket_server.serialization_protocol", newConfig.WebsocketConsumerOptions.SerializationProtocol)

	viper.Set("ticker_transformations", newConfig.TickerTransformationOptions)

	if saveCurrentConfig {
		slog.Info("Saving new config")
		err := viper.WriteConfig()
		if err != nil {
			slog.Error("error saving new config to file", "error", err)
		}
	}
}
