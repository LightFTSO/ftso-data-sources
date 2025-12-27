package serialization

import (
	log "log/slog"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/proto"
	"roselabs.mx/ftso-data-sources/model"
	pbticker "roselabs.mx/ftso-data-sources/model/pb"
)

// convertModelTickersToProto converts a slice of model.Ticker into a slice of protobuf Ticker messages.
func convertModelTickersToProto(tickers []*model.TickerMessage) []*pbticker.TickerMessage {
	protoTickers := make([]*pbticker.TickerMessage, len(tickers))
	for i, t := range tickers {
		protoTickers[i] = &pbticker.TickerMessage{
			Base:      t.Base,
			Quote:     t.Quote,
			Source:    t.Source,
			LastPrice: t.Price,
			Timestamp: t.Timestamp,
		}
	}
	return protoTickers
}

func ProtobufTickerSerializer(tickers []*model.TickerMessage) ([]byte, error) {
	// Convert to protobuf representation and marshal
	protoTickers := convertModelTickersToProto(tickers)
	tickerList := &pbticker.TickerMessageList{
		Tickers: protoTickers,
	}
	protoPayload, err := proto.Marshal(tickerList)
	if err != nil {
		log.Error("error encoding tickers to protobuf", "consumer", "websocket", "error", err)
		return nil, err
	}
	return protoPayload, nil
}
func JsonTickerSerializer(tickers []*model.TickerMessage) ([]byte, error) {
	// JSON payload using sonic.Marshal (default)
	jsonPayload, err := sonic.Marshal(tickers)
	if err != nil {
		log.Error("error encoding tickers to JSON", "consumer", "websocket", "error", err)
		return nil, err
	}
	return jsonPayload, nil
}
