package consumer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

// Prevent OOM: Drop packets if buffer grows beyond this
const MAX_ZMQ_BUFFER_CAPACITY = 10000
const ZMQ_INITIAL_BUFFER_SIZE = 500
const ZMQ_FLUSH_INTERVAL = 50 * time.Millisecond
const ZMQ_BATCH_SIZE = 1024

type ZMQConsumerOptions struct {
	Enabled       bool          `mapstructure:"enabled"`
	Port          int           `mapstructure:"port"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// We default to JSON for now, binary comes later
}

type ZMQConsumer struct {
	socket         *zmq.Socket
	TickerListener *broadcast.Listener

	config ZMQConsumerOptions

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex
}

func (s *ZMQConsumer) setup() error {
	// 1. Create a Publisher Socket
	// ZMQ_PUB: We broadcast messages. Consumers (SUB) can filter by topic.
	sock, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return fmt.Errorf("could not create ZMQ socket: %w", err)
	}

	// 2. Bind to the port (e.g., tcp://*:5555)
	// "*" means bind to all available network interfaces
	address := fmt.Sprintf("tcp://*:%d", s.config.Port)
	if err := sock.Bind(address); err != nil {
		return fmt.Errorf("could not bind ZMQ to address %s: %w", address, err)
	}

	s.socket = sock
	log.Info("ZeroMQ Consumer started", "consumer", "zmq", "address", address)
	return nil
}

func (s *ZMQConsumer) flushBatch() {
	s.mutex.Lock()
	if len(s.tickerBuffer) == 0 {
		s.mutex.Unlock()
		return
	}
	batch := s.tickerBuffer
	s.tickerBuffer = make([]*model.Ticker, 0, ZMQ_BATCH_SIZE)
	s.mutex.Unlock()

	s.sendBinaryBatch(batch)
}

func (s *ZMQConsumer) sendBinaryBatch(tickers []*model.Ticker) {
	// Pre-allocate estimated size: 2 byte (count) + N * (1 byte len + 20 bytes topic + 16 bytes data)
	buf := bytes.NewBuffer(make([]byte, 0, 2+len(tickers)*40))

	// 1. Write Batch Count (uint8)
	// Our CONST is 100, so uint8 is fine.
	count := uint16(len(tickers))
	binary.Write(buf, binary.LittleEndian, count)

	for _, t := range tickers {
		market := fmt.Sprintf("%s:%s:%s", t.Base, t.Source, t.Quote)
		marketBytes := []byte(market)

		// 2. Write Topic Length (uint8)
		buf.WriteByte(uint8(len(marketBytes)))

		// 3. Write Topic String
		buf.Write(marketBytes)

		// 4. Write Price (Little Endian Float64)
		binary.Write(buf, binary.LittleEndian, t.Price)

		// 5. Write Timestamp (Little Endian Int64)
		binary.Write(buf, binary.LittleEndian, t.Timestamp.UnixMilli())
	}

	// Send as a generic "BATCH" topic, or use the first ticker's topic if filtering matters.
	// For your case (Ingesting all), a single "TICKERS" channel is best.
	s.socket.Send("TICKERS", zmq.SNDMORE)
	s.socket.SendBytes(buf.Bytes(), 0)
}

func (s *ZMQConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	log.Debug("ZeroMQ listener started", "consumer", "zmq")

	go func() {
		ticker := time.NewTicker(ZMQ_FLUSH_INTERVAL)
		for range ticker.C {
			s.flushBatch()
		}
	}()

	go func() {
		for t := range s.TickerListener.Channel() {
			ticker := t.(model.Ticker)

			s.mutex.Lock()
			s.tickerBuffer = append(s.tickerBuffer, &ticker)

			// Flush immediately if full (Low Latency)
			if len(s.tickerBuffer) >= ZMQ_BATCH_SIZE {
				// We unlock inside flushBatch, but since we are calling it here:
				// It's cleaner to unlock, then call flush, or handle locking inside flush.
				// Let's handle it manually here for speed:
				batch := s.tickerBuffer
				s.tickerBuffer = make([]*model.Ticker, 0, ZMQ_BATCH_SIZE)
				s.mutex.Unlock()

				s.sendBinaryBatch(batch)
			} else {
				s.mutex.Unlock()
			}
		}
	}()
}

func (s *ZMQConsumer) CloseTickerListener() {
	if s.TickerListener != nil {
		s.TickerListener.Discard()
	}
	if s.socket != nil {
		s.socket.Close()
	}
}

func NewZMQConsumer(options ZMQConsumerOptions) *ZMQConsumer {
	// Default flush interval if not set
	if options.FlushInterval <= 0 {
		options.FlushInterval = 10 * time.Millisecond
	}

	newConsumer := &ZMQConsumer{
		config:       options,
		tickerBuffer: make([]*model.Ticker, 0, ZMQ_INITIAL_BUFFER_SIZE),
	}

	if err := newConsumer.setup(); err != nil {
		// Panic on startup failure is usually acceptable for configuration errors
		panic(err)
	}

	return newConsumer
}
