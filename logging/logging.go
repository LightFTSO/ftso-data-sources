package logging

import (
	"log/slog"
	"os"
)

func SetupLogging() {
	defaultLogger := slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{}),
	)

	slog.SetDefault(defaultLogger)
}
