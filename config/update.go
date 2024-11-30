package config

import (
	"log/slog"

	"github.com/spf13/viper"
)

func UpdateConfig(newConfig ConfigOptions, saveCurrentConfig bool) {
	if saveCurrentConfig {
		SaveConfig()
	}

	viper.Set("env", newConfig.Env)
	viper.Set("log_level", newConfig.LogLevel)

	viper.Set("message_buffer_size", newConfig.MessageBufferSize)

	viper.Set("use_exchange_timestamp", newConfig.UseExchangeTimestamp)

	viper.Set("assets.crypto", newConfig.Assets.Crypto)
	viper.Set("assets.commodities", newConfig.Assets.Commodities)
	viper.Set("assets.forex", newConfig.Assets.Forex)
	viper.Set("assets.stocks", newConfig.Assets.Stocks)

	viper.Set("stats.enabled", newConfig.Stats.Enabled)
	viper.Set("stats.interval", newConfig.Stats.Interval)
	viper.Set("stats.num_threads", newConfig.Stats.NumThreads)

	viper.Set("datasources", newConfig.Datasources)

	viper.Set("file_consumer.enabled", newConfig.FileFileConsumerOptions.Enabled)
	viper.Set("file_consumer.filename", newConfig.FileFileConsumerOptions.OutputFilename)

	viper.Set("mqtt.enabled", newConfig.MQTTConsumerOptions.Enabled)
	viper.Set("mqtt.num_threads", newConfig.MQTTConsumerOptions.NumThreads)
	viper.Set("mqtt.use_sbe_encoding", newConfig.MQTTConsumerOptions.UseSbeEncoding)
	viper.Set("mqtt.qos_level", newConfig.MQTTConsumerOptions.QOSLevel)

	viper.Set("redis.enabled", newConfig.RedisOptions.Enabled)
	viper.Set("redis.client_options.initaddress", newConfig.RedisOptions.ClientOptions.InitAddress)
	viper.Set("redis.client_options.username", newConfig.RedisOptions.ClientOptions.Username)
	viper.Set("redis.client_options.password", newConfig.RedisOptions.ClientOptions.Password)
	viper.Set("redis.num_threads", newConfig.RedisOptions.NumThreads)
	viper.Set("redis.include_stdout", newConfig.RedisOptions.IncludeStdout)
	viper.Set("redis.ts.retention", newConfig.RedisOptions.TsOptions.Retention)
	viper.Set("redis.ts.chunksize", newConfig.RedisOptions.TsOptions.ChunkSize)

	viper.Set("websocket_server.enabled", newConfig.WebsocketConsumerOptions.Enabled)
	viper.Set("websocket_server.use_sbe_encoding", newConfig.WebsocketConsumerOptions.UseSbeEncoding)
	viper.Set("websocket_server.port", newConfig.WebsocketConsumerOptions.Port)
	viper.Set("websocket_server.ticker_endpoint", newConfig.WebsocketConsumerOptions.TickersEndpoint)
	viper.Set("websocket_server.use_sbe_encoding", newConfig.WebsocketConsumerOptions.UseSbeEncoding)

	viper.Set("questdb.enabled", newConfig.QuestDBConsumerOptions.Enabled)
	viper.Set("questdb.flush_interval", newConfig.QuestDBConsumerOptions.FlushInterval)
	viper.Set("questdb.client_options.address", newConfig.QuestDBConsumerOptions.ClientOptions.Address)
	viper.Set("questdb.client_options.schema", newConfig.QuestDBConsumerOptions.ClientOptions.Schema)
	viper.Set("questdb.individual_feed_table", newConfig.QuestDBConsumerOptions.IndividualFeedTable)

	if saveCurrentConfig {
		slog.Info("Saving new config")
		err := viper.WriteConfig()
		if err != nil {
			slog.Error("error saving new config to file", "error", err)
		}
	}

}
