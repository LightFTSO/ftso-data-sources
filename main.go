package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	slog "log/slog"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"

	"roselabs.mx/ftso-data-sources/config"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/flags"
	"roselabs.mx/ftso-data-sources/logging"
	"roselabs.mx/ftso-data-sources/rpcmanager"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

func main() {
	// Parse command-line flags
	flag.Parse()

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
	if globalConfig.UseExchangeTimestamp {
		slog.Info("Using exchange timestamp as ticker timestamp")
	} else {
		slog.Info("Using local timestamp as ticker timestamp")
	}

	slog.Debug(fmt.Sprintf("Ticker broadcaster buffer size is %d", config.Config.MessageBufferSize))
	tickerTopic := tickertopic.NewTickerTopic(config.Config.TickerTransformationOptions, config.Config.MessageBufferSize)

	// Initialize consumers
	initConsumers(tickerTopic, globalConfig)

	// Initialize RPC Manager
	manager := &rpcmanager.RPCManager{
		DataSources:   make(map[string]datasource.FtsoDataSource),
		TickerTopic:   tickerTopic,
		GlobalConfig:  globalConfig,
		CurrentAssets: config.Config.Assets,
	}

	// Initialize data sources
	err := manager.InitDataSources()
	if err != nil {
		log.Fatalf("Failed to initialize data sources: %v", err)
	}

	// Start RPC server
	go startRpcManager(manager)

	// Wait for all data sources, consumers and RPC manager to finish
	manager.Wg.Wait()
}

func startRpcManager(manager *rpcmanager.RPCManager) {
	rpc.Register(manager)
	rpc.HandleHTTP()

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var conn = struct {
			io.Reader
			io.Writer
			io.Closer
		}{r.Body, w, r.Body}

		jsonrpc.ServeConn(conn)
	})

	// Listen on a TCP port, e.g., 1234
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", manager.GlobalConfig.Port))
	if err != nil {
		log.Fatalf("Error starting RPC server: %v", err)
	}
	defer listener.Close()

	slog.Info(fmt.Sprintf("RPC server started on port :%d", manager.GlobalConfig.Port))

	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

func enableConsumer(c consumer.Consumer, tickerTopic *tickertopic.TickerTopic) {
	c.StartTickerListener(tickerTopic)
}

func initConsumers(tickerTopic *tickertopic.TickerTopic, config config.ConfigOptions) {
	if !config.FileConsumerOptions.Enabled &&
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

	if config.FileConsumerOptions.Enabled {
		c := consumer.NewFileConsumer(config.FileConsumerOptions.OutputFilename)
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
