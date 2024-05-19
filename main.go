package main

import (
	"flag"
	"fmt"
	"log"
	slog "log/slog"
	"strings"
	"sync"

	"github.com/textileio/go-threads/broadcast"

	"roselabs.mx/ftso-data-sources/config"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/flags"
	"roselabs.mx/ftso-data-sources/logging"
	"roselabs.mx/ftso-data-sources/symbols"
)

func init() {
	// Parse command-line flags
	flag.Parse()

}

func main() {
	config, err := config.LoadConfig(*flags.ConfigFile)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	logging.SetupLogging(config)

	fmt.Println("=========  FTSO Data Sources  =========")
	slog.Info("Created with <3 by RoseLabs.Mx (LightFTSO)")

	run(config)

	slog.Warn("À Bientôt! Adiós! Goodbye!")
}

func run(globalConfig config.ConfigOptions) {
	tickerTopic := broadcast.NewBroadcaster(config.Config.MessageBufferSize)
	initConsumers(tickerTopic, globalConfig)
	initDataSources(tickerTopic, globalConfig)
}

func enableConsumer(c consumer.Consumer, tickerTopic *broadcast.Broadcaster) {
	c.StartTickerListener(tickerTopic)
}

func initConsumers(tickerTopic *broadcast.Broadcaster, config config.ConfigOptions) {
	if !config.FileFileConsumerOptions.Enabled &&
		!config.RedisOptions.Enabled &&
		!config.WebsocketConsumerOptions.Enabled &&
		!config.MQTTConsumerOptions.Enabled &&
		!config.QuestDBConsumerOptions.Enabled {
		if config.Env != "development" {
			panic("no consumers enabled")
		} else {
			slog.Warn("No consumers enabled, data will go nowhere!")
		}
	}

	if config.RedisOptions.Enabled {
		c := consumer.NewRedisConsumer(config.RedisOptions)
		enableConsumer(c, tickerTopic)
	}

	if config.FileFileConsumerOptions.Enabled {
		c := consumer.NewFileConsumer(config.FileFileConsumerOptions.OutputFilename)
		enableConsumer(c, tickerTopic)
	}

	if config.MQTTConsumerOptions.Enabled {
		c := consumer.NewMqttConsumer(config.MQTTConsumerOptions)
		enableConsumer(c, tickerTopic)
	}

	if config.QuestDBConsumerOptions.Enabled {
		c := consumer.NewQuestDbConsumer(config.QuestDBConsumerOptions)
		enableConsumer(c, tickerTopic)
	}

	if config.WebsocketConsumerOptions.Enabled {
		c := consumer.NewWebsocketConsumer(config.WebsocketConsumerOptions)
		enableConsumer(c, tickerTopic)
	}

	// enable statistics generator
	if config.Stats.Enabled {
		stats := consumer.NewStatisticsGenerator(config.Stats)
		enableConsumer(stats, tickerTopic)
	}

}

func initDataSources(tickerTopic *broadcast.Broadcaster, config config.ConfigOptions) error {
	var w sync.WaitGroup

	allSymbols := symbols.GetAllSymbols(config.Assets.Crypto, config.Assets.Commodities, config.Assets.Forex, config.Assets.Stocks)
	syms := []string{}
	for _, s := range allSymbols.Flatten() {
		syms = append(syms, strings.ToUpper(s.GetSymbol()))
	}
	slog.Debug(fmt.Sprintf("list of enabled feeds: %+v", syms))

	if len(allSymbols.Flatten()) < 1 {
		if config.Env != "development" {
			panic("we aren't watching any assets!")
		} else {
			slog.Warn("No assets defined, no data will be obtained!")
		}
	}

	dataSourceList := config.Datasources

	if len(dataSourceList) < 1 {
		if config.Env != "development" {
			panic("we aren't connecting to any datasources!")
		} else {
			slog.Warn("No data sources enabled, where will get the data from?")
		}
	}

	for _, source := range dataSourceList {
		w.Add(1)
		go func(source datasource.DataSourceOptions) {
			src, err := datasource.BuilDataSource(source, allSymbols, tickerTopic, &w)
			if err != nil {
				slog.Error("Error creating data source", "datasource", source.Source, "error", err.Error())
				w.Done()
				return
			}
			err = src.Connect()
			if err != nil {
				slog.Error("Error connecting", "datasource", src.GetName())
				w.Done()
				return
			}

			w.Done()
		}(source)
	}

	// wait for all datasources to exit
	w.Wait()

	return nil
}
