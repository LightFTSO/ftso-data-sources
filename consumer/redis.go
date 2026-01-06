package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/redis/rueidis"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

const TICKERS_KEY string = "tickers"
const MAX_BUFFER_CAPACITY = 5000

type RedisOptions struct {
	Enabled       bool `mapstructure:"enabled"`
	ClientOptions struct {
		Username    string   `mapstructure:"username"`
		Password    string   `mapstructure:"password"`
		InitAddress []string `mapstructure:"initaddress"`
	} `mapstructure:"client_options"`
	TsOptions struct {
		Retention time.Duration `mapstructure:"retention"`
		ChunkSize int64         `mapstructure:"chunksize"`
		MaxMemory string        `mapstructure:"maxmemory"`
	} `mapstructure:"timeseries"`
}

type RedisConsumer struct {
	TickerListener *broadcast.Listener
	redisClient    rueidis.Client

	tsRetention       time.Duration
	tsChunkSize       int64
	instanceMaxMemory string

	tickerBuffer []*model.Ticker
	mutex        sync.Mutex

	timeSeriesKeys map[string]bool
}

func (s *RedisConsumer) setup() error {
	s.timeSeriesKeys = make(map[string]bool)

	if len(s.instanceMaxMemory) > 0 {
		log.Info("Setting maxmemory configuration value", "consumer", "redis")
		maxMemCmd := s.redisClient.B().
			ConfigSet().
			ParameterValue().
			ParameterValue("maxmemory", s.instanceMaxMemory).
			ParameterValue("maxmemory-policy", "volatile-ttl").
			Build()

		if err := s.redisClient.Do(context.Background(), maxMemCmd).Error(); err != nil {
			log.Warn("Could not set Redis maxmemory (permission denied?)", "consumer", "redis", "error", err)
			// Do NOT panic here. The app can still function.
		}
	}
	return nil
}

func (s *RedisConsumer) processTickerBatch(tickers []*model.Ticker) {
	if len(tickers) == 0 {
		return
	}

	tsMaddCommand := s.redisClient.B().TsMadd().KeyTimestampValue()

	validCommandsCount := 0

	for _, ticker := range tickers {
		key := fmt.Sprintf("%s:%s:%s:%s", TICKERS_KEY, ticker.Source, ticker.Base, ticker.Quote)

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
					DuplicatePolicyLast().
					Labels().
					Labels("source", ticker.Source).
					Labels("base", ticker.Base).
					Labels("quote", ticker.Quote).
					Build()

				err := s.redisClient.Do(context.Background(), cmd).Error()
				if err != nil {
					log.Error("Error creating time series", "key", key, "error", err)
					continue
				}
			}
			// Mark the key as existing
			s.timeSeriesKeys[key] = true
		}

		ts := ticker.Timestamp.UTC().UnixMilli()
		tsMaddCommand = tsMaddCommand.KeyTimestampValue(key, ts, ticker.Price)
		validCommandsCount++
	}

	if validCommandsCount > 0 {
		err := s.redisClient.Do(context.Background(), tsMaddCommand.Build()).Error()
		if err != nil {
			log.Error("Error executing TS.MADD", "consumer", "redis", "error", err)
		}
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
		s.tickerBuffer = make([]*model.Ticker, 0, 500) // Reset with some initial capacity
		s.mutex.Unlock()

		s.processTickerBatch(tickersToProcess)
	}
}

func (s *RedisConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	log.Debug("Redis ticker listener started", "consumer", "redis")
	s.TickerListener = tickerTopic.Broadcaster.Listen()

	go func() {
		for t := range s.TickerListener.Channel() {
			originalTicker := (t.(*model.Ticker))

			tickerCopy := *originalTicker

			s.mutex.Lock()

			if len(s.tickerBuffer) >= MAX_BUFFER_CAPACITY {
				s.mutex.Unlock()
				log.Warn("Redis Consumer buffer full! Dropping ticker.", "base", tickerCopy.Base)
				continue
			}

			s.tickerBuffer = append(s.tickerBuffer, &tickerCopy)
			s.mutex.Unlock()
		}
	}()

	go s.flushTickers()
}

func (s *RedisConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
	s.redisClient.Close()
}

func NewRedisConsumer(options RedisOptions) *RedisConsumer {
	clientOptions := rueidis.ClientOption{
		Username:    options.ClientOptions.Username,
		Password:    options.ClientOptions.Password,
		InitAddress: options.ClientOptions.InitAddress,
	}
	r, err := rueidis.NewClient(clientOptions)
	if err != nil {
		panic(err)
	}

	newConsumer := &RedisConsumer{
		redisClient:       r,
		tsRetention:       options.TsOptions.Retention,
		tsChunkSize:       options.TsOptions.ChunkSize,
		instanceMaxMemory: options.TsOptions.MaxMemory,
		tickerBuffer:      make([]*model.Ticker, 0, 500),
	}
	newConsumer.setup()

	return newConsumer
}
