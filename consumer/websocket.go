package consumer

import (
	log "log/slog"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"golang.org/x/net/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	websocket_server "roselabs.mx/ftso-data-sources/internal/websocket_server"
	"roselabs.mx/ftso-data-sources/model"
)

type WebsocketConsumerOptions struct {
	Enabled         bool          `mapstructure:"enabled"`
	TickersEndpoint string        `mapstructure:"ticker_endpoint"`
	UseSbeEncoding  bool          `mapstructure:"use_sbe_encoding"`
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
	log.Info("Websocket Consumer started.", "port", s.config.Port, "sbe_encoding", s.config.UseSbeEncoding)

	return nil
}

func (s *WebsocketServerConsumer) processTickerBatch(tickers []*model.Ticker) {

	// Marshal the tickers
	payload, err := sonic.Marshal(tickers)
	if err != nil {
		log.Error("error encoding tickers", "consumer", "websocket", "error", err)
		return
	}

	// Broadcast the payload
	err = s.wsServer.BroadcastMessage(websocket.TextFrame, payload)
	if err != nil {
		log.Error("error broadcasting tickers", "consumer", "websocket", "error", err)
	}
}

func (s *WebsocketServerConsumer) processTickerBatchSbe(tickers []*model.Ticker, sbeMarshaller *internal.SbeMarshaller) {
	var payload []byte
	for _, ticker := range tickers {
		encodedTicker, err := sbeMarshaller.MarshalSbe(*ticker)
		if err != nil {
			log.Error("error encoding ticker", "consumer", "websocket", "error", err)
			continue
		}
		encodedTicker = append(encodedTicker, '\n')
		payload = append(payload, encodedTicker...)
	}

	// Broadcast the payload
	err := s.wsServer.BroadcastMessage(websocket.BinaryFrame, payload)
	if err != nil {
		log.Error("error broadcasting tickers", "consumer", "websocket", "error", err)
	}
}

func (s *WebsocketServerConsumer) flushTickers() {
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()
	var sbeMarshaller *internal.SbeMarshaller
	if s.config.UseSbeEncoding {
		marshaller := internal.NewSbeGoMarshaller()
		sbeMarshaller = &marshaller
	}

	for range ticker.C {
		s.mutex.Lock()
		if len(s.tickerBuffer) == 0 {
			s.mutex.Unlock()
			continue
		}
		tickersToProcess := s.tickerBuffer
		s.tickerBuffer = nil // Reset the buffer
		s.mutex.Unlock()

		if s.config.UseSbeEncoding {
			s.processTickerBatchSbe(tickersToProcess, sbeMarshaller)
		} else {
			s.processTickerBatch(tickersToProcess)
		}

	}
}

func (s *WebsocketServerConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers and accumulate them
	s.TickerListener = tickerTopic.Listen()
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

func NewWebsocketConsumer(options WebsocketConsumerOptions, useExchangeTimestamp bool) *WebsocketServerConsumer {
	server := websocket_server.NewWebsocketServer(options.Port, options.TickersEndpoint)

	newConsumer := &WebsocketServerConsumer{
		wsServer:             *server,
		config:               options,
		useExchangeTimestamp: useExchangeTimestamp,
	}
	newConsumer.setup()

	return newConsumer
}
