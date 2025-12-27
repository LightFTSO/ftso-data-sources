package serialization

import (
	log "log/slog"

	pbticker "roselabs.mx/ftso-data-sources/generated/proto/ticker"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"roselabs.mx/ftso-data-sources/model"
)

// convertModelTickersToProto converts a slice of model.Ticker into a slice of protobuf Ticker messages.
func convertModelTickersToProto(tickers []*model.Ticker) []*pbticker.Ticker {
	protoTickers := make([]*pbticker.Ticker, len(tickers))
	for i, t := range tickers {
		protoTickers[i] = &pbticker.Ticker{
			Base:      t.Base,
			Quote:     t.Quote,
			Source:    t.Source,
			LastPrice: t.Price,
			Timestamp: timestamppb.New(t.Timestamp),
		}
	}
	return protoTickers
}

func ProtobufTickerSerializer(tickers []*model.Ticker) ([]byte, error) {
	// Convert to protobuf representation and marshal
	protoTickers := convertModelTickersToProto(tickers)
	tickerList := &pbticker.TickerList{
		Tickers: protoTickers,
	}
	protoPayload, err := proto.Marshal(tickerList)
	if err != nil {
		log.Error("error encoding tickers to protobuf", "consumer", "websocket", "error", err)
		return nil, err
	}
	return protoPayload, nil
}
func JsonTickerSerializer(tickers []*model.Ticker) ([]byte, error) {
	// JSON payload using sonic.Marshal (default)
	jsonPayload, err := sonic.Marshal(tickers)
	if err != nil {
		log.Error("error encoding tickers to JSON", "consumer", "websocket", "error", err)
		return nil, err
	}
	return jsonPayload, nil
}
