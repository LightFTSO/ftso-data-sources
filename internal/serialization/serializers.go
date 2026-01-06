package serialization

import (
	log "log/slog"

	"github.com/bytedance/sonic"
	"roselabs.mx/ftso-data-sources/model"
)

// convertModelTickersToProto converts a slice of model.Ticker into a slice of protobuf Ticker messages.

func JsonTickerSerializer(tickers []model.TickerMessage) ([]byte, error) {
	// JSON payload using sonic.Marshal (default)
	jsonPayload, err := sonic.Marshal(tickers)
	if err != nil {
		log.Error("error encoding tickers to JSON", "consumer", "websocket", "error", err)
		return nil, err
	}
	return jsonPayload, nil
}
