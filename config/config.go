package config

import (
	"path"

	"github.com/spf13/viper"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
)

type ConfigOptions struct {
	Env string `mapstructure:"env"`

	Datasources []datasource.DataSourceOptions `mapstructure:"datasources"`

	Stats consumer.StatisticsGeneratorOptions `mapstructure:"stats"`

	MessageBufferSize int `mapstructure:"message_buffer_size"`

	Assets struct {
		Crypto      []string `mapstructure:"crypto"`
		Commodities []string `mapstructure:"commodities"`
		Forex       []string `mapstructure:"forex"`
		Stocks      []string `mapstructure:"stocks"`
	} `mapstructure:"assets"`

	EnabledStreams struct {
		Trades  bool `mapstructure:"trades"`
		Tickers bool `mapstructure:"tickers"`
	} `mapstructure:"enabled_streams"`

	RedisOptions            consumer.RedisOptions                   `mapstructure:"redis"`
	WebsocketServerOptions  consumer.WebsocketServerConsumerOptions `mapstructure:"websocket_server"`
	FileFileConsumerOptions consumer.FileConsumerOptions            `mapstructure:"file_output"`
}

var Config ConfigOptions

func init() {
	setDefaults()
}

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

	viper.SetDefault("redis.enabled", false)
	viper.SetDefault("redis.client_options.initaddress", []string{"127.0.0.1:6379"})
	viper.SetDefault("redis.client_options.username", "")
	viper.SetDefault("redis.client_options.password", "")
	viper.SetDefault("redis.num_threads", 1)
	viper.SetDefault("redis.include_stdout", false)
	viper.SetDefault("redis.ts.retention", "24h")
	viper.SetDefault("redis.ts.chunksize", 4096)

	viper.SetDefault("websocket_server.enabled", false)
	viper.SetDefault("websocket_server.host", "127.0.0.1")
	viper.SetDefault("websocket_server.port", 3000)
	viper.SetDefault("websocket_server.endpoints.trades", "/trades")
	viper.SetDefault("websocket_server.endpoints.tickers", "/tickers")
	viper.SetDefault("websocket_server.endpoints.volumes", "/volumes")
}

func LoadConfig(configFile string) (config ConfigOptions, err error) {
	viper.AutomaticEnv()
	viper.AddConfigPath(".")

	viper.AddConfigPath(path.Dir(configFile))
	viper.SetConfigFile(path.Base(configFile))

	err = viper.ReadInConfig()
	if err != nil {
		return ConfigOptions{}, err
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return ConfigOptions{}, err
	}

	Config = config
	return config, nil
}
