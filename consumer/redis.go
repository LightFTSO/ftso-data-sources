package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/redis/rueidis"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
)

const TICKERS_KEY string = "tickers"

type RedisOptions struct {
	Enabled       bool
	ClientOptions rueidis.ClientOption `mapstructure:"client_options"`
	TsOptions     struct {
		Retention time.Duration `mapstructure:"include_stdout"`
		ChunkSize int64         `mapstructure:"chunksize"`
		MaxMemory string        `mapstructure:"maxmemory"`
	} `mapstructure:"ts"`
}

type RedisConsumer struct {
	TickerListener *broadcast.Listener

	redisClient rueidis.Client

	tsRetention          time.Duration
	tsChunkSize          int64
	instanceMaxMemory    string
	useExchangeTimestamp bool

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex

	timeSeriesKeys map[string]bool
}

func (s *RedisConsumer) setup() error {
	log.Info("Setting maxmemory configuration value", "consumer", "redis", "maxmemory", s.instanceMaxMemory, "maxmemory-policy", "volatile-ttl")
	if len(s.instanceMaxMemory) <= 0 {
		log.Warn("Redis's config param maxmemory is empty. Please check memory usage", "consumer", "redis")
	}
	maxMemCmd := s.redisClient.B().
		ConfigSet().
		ParameterValue().
		ParameterValue("maxmemory", s.instanceMaxMemory).
		ParameterValue("maxmemory-policy", "volatile-ttl").
		Build()

	if err := s.redisClient.Do(context.Background(), maxMemCmd).Error(); err != nil {
		log.Error("Error setting maxmemory", "consumer", "redis", "error", err)
		panic(err)
	}

	// initialize
	s.timeSeriesKeys = make(map[string]bool)

	log.Info("Creating informational keys", "consumer", "redis")

	cmd := s.redisClient.B().Keys().Pattern(fmt.Sprintf("%s:*", TICKERS_KEY)).Build()
	tsKeys, err := s.redisClient.Do(context.Background(), cmd).AsStrSlice()
	if err != nil {
		log.Error("Error creating meta informational keys", "consumer", "redis")
		return err
	}

	log.Info("Updating retention rules", "consumer", "redis")
	for _, key := range tsKeys {
		cmd := s.redisClient.B().TsAlter().Key(key).Retention(s.tsRetention.Milliseconds()).ChunkSize(s.tsChunkSize).DuplicatePolicyLast().Build()
		s.redisClient.Do(context.Background(), cmd)
	}

	return nil

}

func (s *RedisConsumer) processTickerBatch(tickers []*model.Ticker) {
	tsMaddCommand := s.redisClient.B().TsMadd().KeyTimestampValue()

	for _, ticker := range tickers {
		val, err := strconv.ParseFloat(ticker.LastPrice, 64)
		if err != nil {
			log.Error("Error parsing float", "value", ticker.LastPrice, "consumer", "redis", "error", err)
			continue
		}
		key := fmt.Sprintf("%s:%s:%s:%s", TICKERS_KEY, ticker.Source, ticker.Base, ticker.Quote)

		// Check if the key exists in our map
		if !s.timeSeriesKeys[key] {
			// Check if key exists in Redis
			cmd := s.redisClient.B().Exists().Key(key).Build()
			exists, err := s.redisClient.Do(context.Background(), cmd).AsBool()
			if err != nil {
				log.Error("Error checking key existence", "consumer", "redis", "key", key, "error", err)
				continue
			}
			if !exists {
				// Create the time series
				cmd := s.redisClient.B().
					TsCreate().
					Key(key).
					Retention(s.tsRetention.Milliseconds()).
					EncodingCompressed().
					ChunkSize(s.tsChunkSize).
					DuplicatePolicyFirst().
					Labels().
					Labels("source", ticker.Source).
					Labels("base", ticker.Base).
					Labels("quote", ticker.Quote).
					Build()
				err := s.redisClient.Do(context.Background(), cmd).Error()
				if err != nil {
					log.Error("Error creating time series", "consumer", "redis", "key", key, "error", err)
					continue
				}
			}
			// Mark the key as existing
			s.timeSeriesKeys[key] = true
		}

		ts := ticker.Timestamp.UTC().UnixMilli()

		tsMaddCommand = tsMaddCommand.KeyTimestampValue(key, ts, val)
	}

	err := s.redisClient.Do(context.Background(), tsMaddCommand.Build()).Error()
	if err != nil {
		log.Error("Error executing TS.MADD", "consumer", "redis", "error", err)
	}

}

func (s *RedisConsumer) flushTickers() {
	tickerInterval := time.NewTicker(time.Duration(200 * time.Millisecond))
	defer tickerInterval.Stop()

	for range tickerInterval.C {
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

func (s *RedisConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers in the ch channel and sends them to a io.Writer
	log.Debug("Redis ticker listener started", "consumer", "redis")
	s.TickerListener = tickerTopic.Listen()
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
func (s *RedisConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
	s.redisClient.Close()
}

func NewRedisConsumer(options RedisOptions, useExchangeTimestamp bool) *RedisConsumer {
	r, err := rueidis.NewClient(options.ClientOptions)
	if err != nil {
		panic(err)
	}

	newConsumer := &RedisConsumer{
		redisClient:          r,
		tsRetention:          options.TsOptions.Retention,
		tsChunkSize:          options.TsOptions.ChunkSize,
		instanceMaxMemory:    options.TsOptions.MaxMemory,
		useExchangeTimestamp: useExchangeTimestamp,
	}
	newConsumer.setup()

	return newConsumer
}
