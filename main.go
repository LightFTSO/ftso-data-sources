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
		log.Fatalf("%v\n", err.Error())
	}

	logging.SetupLogging(config)

	slog.Info("--- FTSO Data Sources v1.0 ---")
	slog.Info("Created with <3 by RoseLabs.Mx (LightFTSO)")
	slog.Info("Get in contact at x.com @lightftso or @roselabs.mx")

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
		!config.WebsocketServerOptions.Enabled &&
		!config.MosquittoConsumerOptions.Enabled &&
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

	if config.MosquittoConsumerOptions.Enabled {
		c := consumer.NewMqttConsumer(config.MosquittoConsumerOptions)
		enableConsumer(c, tickerTopic)
	}

	if config.QuestDBConsumerOptions.Enabled {
		c := consumer.NewQuestDbConsumer(config.QuestDBConsumerOptions)
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
		syms = append(syms, strings.ToUpper(s.Symbol))
	}
	slog.Debug(fmt.Sprintf("list of enabled feeds: %+v", syms))

	dataSourceList := config.Datasources
	for _, source := range dataSourceList {
		w.Add(1)
		go func(source datasource.DataSourceOptions) {
			src, err := datasource.BuilDataSource(source, allSymbols, tickerTopic, &w)
			if err != nil {
				slog.Error("Error creating data source", "datasource", source, "error", err.Error())
				w.Done()
				return
			}
			err = src.Connect()
			if err != nil {
				slog.Error("Error connecting", "datasource", src.GetName())
				w.Done()
				return
			}

			if err := src.SubscribeTickers(); err != nil {
				slog.Error("Error subscribing to tickers", "datasource", src.GetName())
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
