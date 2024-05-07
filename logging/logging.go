package logging

import (
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
	}

	defaultLogger := slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}),
	)

	slog.SetDefault(defaultLogger)
}
