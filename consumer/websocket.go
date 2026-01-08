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

const MAX_WS_BUFFER_CAPACITY = 10000
const INITIAL_BUFFER_SIZE = 1000

type WebsocketConsumerOptions struct {
	Enabled               bool          `mapstructure:"enabled"`
	TickersEndpoint       string        `mapstructure:"ticker_endpoint"`
	FlushInterval         time.Duration `mapstructure:"flush_interval"`
	SerializationProtocol string        `mapstructure:"serialization_protocol"`
}

type WebsocketServerConsumer struct {
	wsServer       websocket_server.WebsocketServer
	TickerListener *broadcast.Listener

	config WebsocketConsumerOptions

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex

	serializer  func([]model.TickerMessage) ([]byte, error)
	messageType int

	port int
}

func (s *WebsocketServerConsumer) getSerializer() (func(tickers []model.TickerMessage) ([]byte, error), error) {
	switch s.config.SerializationProtocol {
	case "json":
		s.messageType = websocket.TextMessage
		return serialization.JsonTickerSerializer, nil
	default:
		err := fmt.Errorf("unknown serialization method %s", s.config.SerializationProtocol)
		return nil, err
	}
}

func (s *WebsocketServerConsumer) setup() error {
	serializer, err := s.getSerializer()
	if err != nil {
		return err
	}
	s.serializer = serializer

	if err := s.wsServer.Connect(); err != nil {
		return err
	}
	log.Info("Websocket Consumer started.", "consumer", "websocket", "port", s.port, "serializer", s.config.SerializationProtocol)

	return nil
}

func (s *WebsocketServerConsumer) processTickerBatch(tickers []*model.Ticker) {
	if len(tickers) == 0 {
		return
	}

	tickersMessage := make([]model.TickerMessage, len(tickers))
	for i, t := range tickers {
		tm := model.NewTickerMessage(*t)
		tickersMessage[i] = tm
	}

	// Marshal the tickers
	payload, err := s.serializer(tickersMessage)
	if err != nil {
		log.Error("error encoding tickers", "consumer", "websocket", "error", err)
		return
	}

	// Broadcast the payload
	s.wsServer.BroadcastBytes(websocket.TextMessage, payload)
}

func (s *WebsocketServerConsumer) flushTickers() {
	interval := time.NewTicker(s.config.FlushInterval)
	defer interval.Stop()

	for range interval.C {
		if !s.wsServer.HasClients() {
			continue
		}

		s.mutex.Lock()
		if len(s.tickerBuffer) == 0 {
			s.mutex.Unlock()
			continue
		}
		tickersToProcess := s.tickerBuffer

		s.tickerBuffer = make([]*model.Ticker, 0, INITIAL_BUFFER_SIZE)
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
			if !s.wsServer.HasClients() {
				continue
			}

			ticker := (t.(model.Ticker))

			s.mutex.Lock()

			if len(s.tickerBuffer) >= MAX_WS_BUFFER_CAPACITY {
				s.mutex.Unlock()
				log.Warn("Websocket Consumer buffer full! Dropping ticker.", "consumer", "websocket")
				continue
			}

			s.tickerBuffer = append(s.tickerBuffer, &ticker)
			s.mutex.Unlock()
		}
	}()

	// Start the flush goroutine
	go s.flushTickers()
}

func (s *WebsocketServerConsumer) CloseTickerListener() {
	if s.TickerListener != nil {
		s.TickerListener.Discard()
	}
}

func NewWebsocketConsumer(options WebsocketConsumerOptions, port int) *WebsocketServerConsumer {
	server := websocket_server.NewWebsocketServer(port, options.TickersEndpoint)

	newConsumer := &WebsocketServerConsumer{
		wsServer:     *server,
		config:       options,
		tickerBuffer: make([]*model.Ticker, 0, INITIAL_BUFFER_SIZE),
		port:         port,
	}

	if err := newConsumer.setup(); err != nil {
		panic(err)
	}

	return newConsumer
}
