package logging

import (
	"fmt"
	"log/slog"
	"os"

	"roselabs.mx/ftso-data-sources/config"
)

func SetupLogging(Config config.ConfigOptions) {
	var logLevel slog.Leveler

	switch Config.LogLevel {
	case "error":
		logLevel = slog.LevelError
	case "warn":
		logLevel = slog.LevelWarn
	case "info":
		logLevel = slog.LevelInfo
	case "debug":
		logLevel = slog.LevelDebug
	default:
		panic(fmt.Sprintf("Unknown log level \"%s\"", Config.LogLevel))
	}

	defaultLogger := slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	)

	slog.SetDefault(defaultLogger)
}

func NewDatasourceLogger(datasourceName string) *slog.Logger {
	dataSourceLogger := slog.Default().With(slog.String("datasource", datasourceName))

	return dataSourceLogger
}
