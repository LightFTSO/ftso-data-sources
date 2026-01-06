package config

import (
	"fmt"
	"os"
	"path"

	"github.com/spf13/viper"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type AssetConfig struct {
	Crypto      []string `mapstructure:"crypto" json:"crypto"`
	Commodities []string `mapstructure:"commodities" json:"commodities"`
	Forex       []string `mapstructure:"forex" json:"forex"`
	Stocks      []string `mapstructure:"stocks" json:"stocks"`
}

type ConfigOptions struct {
	Env string `mapstructure:"env"`

	LogLevel string `mapstructure:"log_level"`

	MessageBufferSize int `mapstructure:"message_buffer_size"`

	Port int `mapstructure:"port"`

	Datasources []datasource.DataSourceOptions `mapstructure:"datasources"`

	Assets AssetConfig `mapstructure:"assets"`

	Stats consumer.StatisticsGeneratorOptions `mapstructure:"stats"`

	RedisOptions             consumer.RedisOptions             `mapstructure:"redis_ts"`
	WebsocketConsumerOptions consumer.WebsocketConsumerOptions `mapstructure:"websocket_server"`
	FileConsumerOptions      consumer.FileConsumerOptions      `mapstructure:"file_output"`
	ZMQConsumerOptions       consumer.ZMQConsumerOptions       `mapstructure:"zeromq"`

	TickerTransformationOptions []tickertopic.TransformationOptions `mapstructure:"ticker_transformations"`
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
	viper.SetConfigType("yaml")

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

// SafeWriteConfig writes the config to a temp file and atomically moves it
// to the destination. This prevents file corruption on crash.
func WriteConfig(filename string) error {
	// 1. Define a temporary filename (e.g., config.yaml.tmp)
	tempFile := "tmp." + filename

	// 2. Write the viper config to the temporary file
	if err := viper.WriteConfigAs(tempFile); err != nil {
		return fmt.Errorf("failed to write temp config: %w", err)
	}

	// 3. Atomically rename temp file to original file
	// This operation is atomic on POSIX systems (Linux/macOS) and safe on Windows
	// (if the file exists, it is replaced).
	if err := os.Rename(tempFile, filename); err != nil {
		return fmt.Errorf("failed to move temp config to %s: %w", filename, err)
	}

	return nil
}
