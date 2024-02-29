package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	slog "log/slog"
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

	logging.SetupLogging()

	fmt.Println("Hell yeahhh")
	fmt.Println("GO GO GO GO GO GO GO  LIGHTFTSO LIGHTFTSO LIGHTFTSO LIGHTFTSO !!! GO GO GO GO GO GO GO GO ")

	run(config)

	fmt.Println("Bye!")

}

func run(globalConfig config.ConfigOptions) {
	tradeTopic := broadcast.NewBroadcaster(config.Config.MessageBufferSize)  //make(chan model.Trade, config.Config.MessageBufferSize)
	tickerTopic := broadcast.NewBroadcaster(config.Config.MessageBufferSize) //make(chan model.Ticker, config.Config.MessageBufferSize)
	initConsumers(tradeTopic, tickerTopic, globalConfig)
	initDataSources(tradeTopic, tickerTopic, globalConfig)
}

func enableConsumer(c consumer.Consumer, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, config config.ConfigOptions) {
	if config.EnabledStreams.Trades {
		c.StartTradeListener(tradeTopic)
	}
	if config.EnabledStreams.Tickers {
		c.StartTickerListener(tickerTopic)
	}
}

func initConsumers(tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, config config.ConfigOptions) {
	if !config.FileFileConsumerOptions.Enabled && !config.RedisOptions.Enabled && !config.WebsocketServerOptions.Enabled && !config.MosquittoConsumerOptions.Enabled {
		if config.Env != "development" {
			err := errors.New("no consumers enabled")
			panic(err)
		} else {
			slog.Warn("No consumers enabled, data will go nowhere!")
		}
	}

	if config.RedisOptions.Enabled {
		c := consumer.NewRedisConsumer(config.RedisOptions)
		enableConsumer(c, tradeTopic, tickerTopic, config)
	}

	if config.FileFileConsumerOptions.Enabled {
		c := consumer.NewFileConsumer(config.FileFileConsumerOptions.OutputFilename)
		enableConsumer(c, tradeTopic, tickerTopic, config)
	}

	if config.MosquittoConsumerOptions.Enabled {
		c := consumer.NewMqttConsumer(config.MosquittoConsumerOptions)
		enableConsumer(c, tradeTopic, tickerTopic, config)
	}

	// enable statistics generator
	if config.Stats.Enabled {
		stats := consumer.NewStatisticsGenerator(config.Stats)
		enableConsumer(stats, tradeTopic, tickerTopic, config)
	}

}

func initDataSources(tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, config config.ConfigOptions) error {
	var w sync.WaitGroup

	allSymbols := symbols.GetAllSymbols()
	dataSourceList := config.Datasources
	for _, source := range dataSourceList {
		w.Add(1)
		go func(source datasource.DataSourceOptions) {
			src, err := datasource.BuilDataSource(source, allSymbols, tradeTopic, tickerTopic, &w)
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

			if config.EnabledStreams.Trades {
				if err := src.SubscribeTrades(); err != nil {
					slog.Error("Error subscribing to trades", "datasource", src.GetName())
					w.Done()
					return
				}
			}
			if config.EnabledStreams.Tickers {
				if err := src.SubscribeTickers(); err != nil {
					slog.Error("Error subscribing to trades", "datasource", src.GetName())
					w.Done()
					return
				}
			}
			w.Done()
		}(source)
	}

	// wait for all datasources to exit
	w.Wait()

	return nil
}
