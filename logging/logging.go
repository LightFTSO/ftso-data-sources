package logging

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"roselabs.mx/ftso-data-sources/config"
)

// SetupLogging installs the process-wide default logger.
func SetupLogging(cfg config.ConfigOptions) {
	level := parseLevel(cfg.LogLevel)

	// Common handler options
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {

			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String(slog.TimeKey, t.UTC().Format(time.RFC3339Nano))
				}
				return a

			case slog.LevelKey:
				if lv, ok := a.Value.Any().(slog.Level); ok {
					return slog.String(slog.LevelKey, strings.ToUpper(lv.String()))
				}
				return a

			// Reduce source noise: only show file:line
			case slog.SourceKey:
				if src, ok := a.Value.Any().(*slog.Source); ok && src != nil {
					src.File = shortenPath(src.File, 1)
					return slog.Any(slog.SourceKey, src)
				}
				return a
			}

			if a.Value.Kind() == slog.KindAny {
				if err, ok := a.Value.Any().(error); ok && err != nil {
					return slog.String(a.Key, err.Error())
				}
			}

			return a
		},
	}

	var h slog.Handler
	switch strings.ToLower(getLogFormat(cfg)) {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	default:
		h = slog.NewTextHandler(os.Stderr, opts)
	}

	base := slog.New(h)

	slog.SetDefault(base)
}

func NewDatasourceLogger(datasourceName string) *slog.Logger {
	return slog.Default().With(slog.String("datasource", datasourceName))
}

func NewComponentLogger(component string) *slog.Logger {
	return slog.Default().With(slog.String("component", component))
}

func WithCtx(ctx context.Context, l *slog.Logger) *slog.Logger { return l }

// ---- helpers ----

func parseLevel(s string) slog.Leveler {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "error":
		return slog.LevelError
	case "warn", "warning":
		return slog.LevelWarn
	case "info":
		return slog.LevelInfo
	case "debug":
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

func shortenPath(p string, n int) string {
	p = filepath.ToSlash(p)
	parts := strings.Split(p, "/")
	if n <= 0 || len(parts) <= n {
		return p
	}
	return strings.Join(parts[len(parts)-n:], "/")
}

func getLogFormat(cfg config.ConfigOptions) string {
	if v := strings.TrimSpace(os.Getenv("LOG_FORMAT")); v != "" {
		return v
	}
	return "json"
}
