package consumer

import (
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal/serialization"
	websocket_server "roselabs.mx/ftso-data-sources/internal/websocket_server"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type WebsocketConsumerOptions struct {
	Enabled               bool          `mapstructure:"enabled"`
	TickersEndpoint       string        `mapstructure:"ticker_endpoint"`
	FlushInterval         time.Duration `mapstructure:"flush_interval"`
	SerializationProtocol string        `mapstructure:"serialization_protocol"`
	Port                  int
}

type WebsocketServerConsumer struct {
	wsServer       websocket_server.WebsocketServer
	TickerListener *broadcast.Listener

	config               WebsocketConsumerOptions
	useExchangeTimestamp bool

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex

	serializer  func([]*model.Ticker) ([]byte, error)
	messageType int
}

func (s *WebsocketServerConsumer) getSerializer() (func(tickers []*model.Ticker) ([]byte, error), error) {
	switch s.config.SerializationProtocol {
	case "json":
		s.messageType = websocket.TextMessage
		return serialization.JsonTickerSerializer, nil
	case "protobuf":
		s.messageType = websocket.BinaryMessage
		return serialization.ProtobufTickerSerializer, nil
	default:
		err := fmt.Errorf("unknown serialization method %s", s.config.SerializationProtocol)
		return nil, err
	}
}

func (s *WebsocketServerConsumer) setup() error {
	serializer, err := s.getSerializer()
	if err != nil {
		panic(err)
	}
	s.serializer = serializer

	if err := s.wsServer.Connect(); err != nil {
		panic(err)
	}
	log.Info("Websocket Consumer started.", "port", s.config.Port, "serializer", s.config.SerializationProtocol)

	return nil
}

func (s *WebsocketServerConsumer) processTickerBatch(tickers []*model.Ticker) {
	// Marshal the tickers
	payload, err := s.serializer(tickers)
	if err != nil {
		log.Error("error encoding tickers", "consumer", "websocket", "error", err)
		return
	}

	// Broadcast the payload
	err = s.wsServer.BroadcastMessage(s.messageType, payload)
	if err != nil {
		log.Error("error broadcasting tickers", "consumer", "websocket", "error", err)
	}
}

func (s *WebsocketServerConsumer) flushTickers() {
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.mutex.Lock()
		if len(s.tickerBuffer) == 0 {
			s.mutex.Unlock()
			continue
		}
		tickersToProcess := s.tickerBuffer
		s.tickerBuffer = nil // Reset the buffer
		s.mutex.Unlock()

		s.processTickerBatch(tickersToProcess)

	}
}

func (s *WebsocketServerConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	// Listen for tickers and accumulate them
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	log.Debug("Websocket ticker listening for tickers now", "consumer", "websocket", "address", s.wsServer.Address)
	go func() {
		for t := range s.TickerListener.Channel() {
			ticker := (t.(*model.Ticker))
			if !s.useExchangeTimestamp {
				ticker.Timestamp = time.Now().UTC()

			}
			s.mutex.Lock()
			s.tickerBuffer = append(s.tickerBuffer, ticker)
			s.mutex.Unlock()
		}
	}()
	// Start the flush goroutine
	go s.flushTickers()
}

func (s *WebsocketServerConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewWebsocketConsumer(options WebsocketConsumerOptions) *WebsocketServerConsumer {
	server := websocket_server.NewWebsocketServer(options.Port, options.TickersEndpoint)

	newConsumer := &WebsocketServerConsumer{
		wsServer: *server,
		config:   options,
	}
	newConsumer.setup()

	return newConsumer
}
