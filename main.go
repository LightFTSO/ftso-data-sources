package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"roselabs.mx/ftso-data-sources/config"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/flags"
	"roselabs.mx/ftso-data-sources/logging"
	"roselabs.mx/ftso-data-sources/rpcmanager"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

func main() {
	flag.Parse()

	cfg, err := config.LoadConfig(*flags.ConfigFile)
	if err != nil {
		log.Fatalf("Failed to load config: %s\n", err)
	}

	logging.SetupLogging(cfg)

	// Context for Graceful Shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("=========  FTSO Data Sources  =========")
	slog.Info("Created with <3 by RoseLabs.Mx (LightFTSO)")

	if err := run(ctx, cfg); err != nil {
		slog.Error("Application exited with error", "error", err)
		os.Exit(1)
	}

	slog.Info("À Bientôt! Adiós! Goodbye!")
}

func run(ctx context.Context, cfg config.ConfigOptions) error {
	slog.Debug("Initializing components", "buffer_size", cfg.MessageBufferSize)

	tickerTopic := tickertopic.NewTickerTopic(
		cfg.TickerTransformationOptions,
		cfg.MessageBufferSize,
	)

	// Initialize Consumers (and register Websocket routes to DefaultServeMux)
	consumerClosers := initConsumers(tickerTopic, cfg)

	// Initialize RPC Manager
	manager := &rpcmanager.RPCManager{
		DataSources:   make(map[string]datasource.FtsoDataSource),
		TickerTopic:   tickerTopic,
		GlobalConfig:  cfg,
		CurrentAssets: cfg.Assets,
		Wg:            &sync.WaitGroup{},
	}

	if err := manager.InitDataSources(); err != nil {
		return fmt.Errorf("failed to init data sources: %w", err)
	}

	// Register RPC Handlers to DefaultServeMux
	registerRpcHandlers(manager)

	// Setup HTTP Server (using DefaultServeMux via Handler: nil)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: nil, // Use DefaultServeMux for both RPC and Websockets
		BaseContext: func(l net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		slog.Info("Server listening (RPC + WebSocket)", "port", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
		}
	}()

	// Wait for CTRL+C
	slog.Info("System is running. Press Ctrl+C to stop.")
	<-ctx.Done()

	// --- Graceful Shutdown Sequence ---
	slog.Info("Shutdown signal received, stopping components...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 1. Stop HTTP Server (stops new connections)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP server shutdown error", "error", err)
	}

	// 2. Close Data Sources (using the new thread-safe method)
	// This will log "Stopping data source..." for each source at INFO level
	manager.CloseAll()

	// 3. Close Consumers
	for _, c := range consumerClosers {
		if closer, ok := c.(io.Closer); ok {
			closer.Close()
		}
	}

	// 4. Wait for background routines to cleanup
	done := make(chan struct{})
	go func() {
		manager.Wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All components stopped cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("Shutdown timed out, forcing exit")
	}

	return nil
}

func registerRpcHandlers(manager *rpcmanager.RPCManager) {
	rpcServer := rpc.NewServer()
	rpcServer.Register(manager)

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("content-type", "application/json")
		conn := struct {
			io.ReadCloser
			io.Writer
		}{r.Body, w}
		rpcServer.ServeCodec(jsonrpc.NewServerCodec(conn))
	})
}

func initConsumers(tickerTopic *tickertopic.TickerTopic, cfg config.ConfigOptions) []consumer.Consumer {
	var activeConsumers []consumer.Consumer

	if !cfg.FileConsumerOptions.Enabled &&
		!cfg.RedisOptions.Enabled &&
		!cfg.WebsocketConsumerOptions.Enabled &&
		!cfg.ZMQConsumerOptions.Enabled {

		if cfg.Env != "development" {
			slog.Error("FATAL: No consumers enabled in production!")
			os.Exit(1)
		} else {
			slog.Warn("No consumers enabled (Development Mode)")
		}
		return nil
	}

	start := func(c consumer.Consumer) {
		c.StartTickerListener(tickerTopic)
		activeConsumers = append(activeConsumers, c)
	}

	if cfg.RedisOptions.Enabled {
		start(consumer.NewRedisConsumer(cfg.RedisOptions))
	}

	if cfg.FileConsumerOptions.Enabled {
		start(consumer.NewFileConsumer(cfg.FileConsumerOptions.OutputFilename))
	}

	if cfg.ZMQConsumerOptions.Enabled {
		start(consumer.NewZMQConsumer(cfg.ZMQConsumerOptions))
	}

	if cfg.WebsocketConsumerOptions.Enabled {
		start(consumer.NewWebsocketConsumer(cfg.WebsocketConsumerOptions, cfg.Port))
	}

	if cfg.Stats.Enabled {
		start(consumer.NewStatisticsGenerator(cfg.Stats))
	}

	return activeConsumers
}
