package consumer

import (
	log "log/slog"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/golang/protobuf/proto"
	"github.com/textileio/go-threads/broadcast"
	"golang.org/x/net/websocket"
	websocket_server "roselabs.mx/ftso-data-sources/internal/websocket_server"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type WebsocketConsumerOptions struct {
	Enabled         bool          `mapstructure:"enabled"`
	TickersEndpoint string        `mapstructure:"ticker_endpoint"`
	FlushInterval   time.Duration `mapstructure:"flush_interval"`
	Port            int
}

type WebsocketServerConsumer struct {
	wsServer       websocket_server.WebsocketServer
	TickerListener *broadcast.Listener

	config               WebsocketConsumerOptions
	useExchangeTimestamp bool

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex
}

func (s *WebsocketServerConsumer) setup() error {
	if err := s.wsServer.Connect(); err != nil {
		panic(err)
	}
	log.Info("Websocket Consumer started.", "port", s.config.Port)

	return nil
}

func (s *WebsocketServerConsumer) processTickerBatch(tickers []*model.Ticker) {
	// Get active connections and their formats
	connections := s.wsServer.GetActiveConnections()

	for conn, useProto := range connections {
		var payload []byte
		var err error

		if useProto {
			// Marshal to protobuf
			payload, err = proto.Marshal(&model.TickerBatch{Tickers: tickers})
			if err != nil {
				log.Error("error encoding tickers to protobuf", "consumer", "websocket", "error", err)
				continue
			}
		} else {
			// Marshal to JSON
			payload, err = sonic.Marshal(tickers)
			if err != nil {
				log.Error("error encoding tickers to json", "consumer", "websocket", "error", err)
				continue
			}
		}

		// Send to specific connection
		err = s.wsServer.SendMessage(conn, websocket.BinaryFrame, payload)
		if err != nil {
			log.Error("error sending tickers", "consumer", "websocket", "error", err)
		}
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
