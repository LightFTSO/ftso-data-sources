package config

import (
	"path"

	"github.com/spf13/viper"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
)

type AssetConfig struct {
	Crypto      []string `mapstructure:"crypto"`
	Commodities []string `mapstructure:"commodities"`
	Forex       []string `mapstructure:"forex"`
	Stocks      []string `mapstructure:"stocks"`
}
type ConfigOptions struct {
	Env string `mapstructure:"env"`

	LogLevel string `mapstructure:"log_level"`

	MessageBufferSize int `mapstructure:"message_buffer_size"`

	UseExchangeTimestamp bool `mapstructure:"use_exchange_timestamp"`

	RPCPort int `mapstructure:"rpc_port"`

	Datasources []datasource.DataSourceOptions `mapstructure:"datasources"`

	Assets AssetConfig `mapstructure:"assets"`

	Stats consumer.StatisticsGeneratorOptions `mapstructure:"stats"`

	RedisOptions             consumer.RedisOptions             `mapstructure:"redis_ts"`
	WebsocketConsumerOptions consumer.WebsocketConsumerOptions `mapstructure:"websocket_server"`
	FileFileConsumerOptions  consumer.FileConsumerOptions      `mapstructure:"file_output"`
	MQTTConsumerOptions      consumer.MqttConsumerOptions      `mapstructure:"mqtt"`
	QuestDBConsumerOptions   consumer.QuestDbConsumerOptions   `mapstructure:"questdb"`
}

var Config ConfigOptions

func init() {
	setDefaults()
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
