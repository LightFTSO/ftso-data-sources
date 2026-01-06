package config

import (
	"fmt"
	"log/slog"

	"github.com/spf13/viper"
)

func UpdateConfig(newConfig ConfigOptions) {
	fmt.Printf("%+v\n", newConfig)

	// Core
	viper.Set("env", newConfig.Env)
	viper.Set("log_level", newConfig.LogLevel)
	viper.Set("message_buffer_size", newConfig.MessageBufferSize)
	viper.Set("port", newConfig.Port)

	// Assets
	viper.Set("assets", newConfig.Assets)

	// Stats
	viper.Set("stats", newConfig.Stats)

	// Datasources
	viper.Set("datasources", newConfig.Datasources)

	// File Output
	viper.Set("file_output", newConfig.FileConsumerOptions)

	// Redis
	viper.Set("redis_ts.enabled", newConfig.RedisOptions.Enabled)
	viper.Set("redis_ts.client_options", newConfig.RedisOptions.ClientOptions)
	viper.Set("redis_ts.ts", newConfig.RedisOptions.TsOptions)

	// Websocket
	viper.Set("websocket_server", newConfig.WebsocketConsumerOptions)

	// ZeroMQ
	viper.Set("zeromq", newConfig.ZMQConsumerOptions)

	// Ticker Transformations
	viper.Set("ticker_transformations", newConfig.TickerTransformationOptions)

	slog.Info("Persisting new configuration to disk")

	// Get the actual config file path from viper (defaulting if not set)
	configFile := viper.ConfigFileUsed()
	if configFile == "" {
		configFile = "config.yaml"
	}

	if err := WriteConfig(configFile); err != nil {
		slog.Error("CRITICAL: Failed to save config file", "error", err)
	}
}
