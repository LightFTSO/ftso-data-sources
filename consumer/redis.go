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
	"roselabs.mx/ftso-data-sources/tickertopic"
)

const (
	TICKERS_KEY         string = "tickers"
	MAX_BUFFER_CAPACITY        = 5000
	FLUSH_INTERVAL             = 50 * time.Millisecond
	REDIS_TIMEOUT              = 1 * time.Second
)

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

	// Control channels
	stopChan chan struct{}
	doneChan chan struct{}
}

func (s *RedisConsumer) setup() error {
	s.timeSeriesKeys = make(map[string]bool)

	if len(s.instanceMaxMemory) > 0 {
		log.Info("Setting maxmemory configuration value", "consumer", "redis")
		// Corrected Builder chain: Removed empty ParameterValue() call
		maxMemCmd := s.redisClient.B().
			ConfigSet().
			ParameterValue().
			ParameterValue("maxmemory", s.instanceMaxMemory).
			ParameterValue("maxmemory-policy", "volatile-ttl").
			Build()

		ctx, cancel := context.WithTimeout(context.Background(), REDIS_TIMEOUT)
		defer cancel()

		if err := s.redisClient.Do(ctx, maxMemCmd).Error(); err != nil {
			log.Warn("Could not set Redis maxmemory (permission denied?)", "consumer", "redis", "error", err)
		}
	}
	return nil
}

func (s *RedisConsumer) processTickerBatch(tickers []*model.Ticker) {
	if len(tickers) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), REDIS_TIMEOUT)
	defer cancel()

	// 1. Separate "New Keys" from "Known Keys"
	// We optimize by assuming keys exist. If we haven't seen them locally,
	// we use TS.ADD (which creates). If we have, we use TS.MADD (faster).
	var newKeyCmds []rueidis.Completed
	var maddTickers []*model.Ticker

	for _, ticker := range tickers {
		key := fmt.Sprintf("%s:%s:%s:%s", TICKERS_KEY, ticker.Source, ticker.Base, ticker.Quote)
		ts := ticker.Timestamp.UTC().UnixMilli()

		if s.timeSeriesKeys[key] {
			// Known key: Add to the bulk MADD list
			maddTickers = append(maddTickers, ticker)
		} else {
			// Unknown key: Create a TS.ADD command (handles Create + Add)
			cmd := s.redisClient.B().
				TsAdd().
				Key(key).
				Timestamp(strconv.FormatInt(ts, 10)).
				Value(ticker.Price).
				Retention(s.tsRetention.Milliseconds()).
				EncodingCompressed().
				ChunkSize(s.tsChunkSize).
				OnDuplicateLast().
				Labels().
				Labels("source", ticker.Source).
				Labels("base", ticker.Base).
				Labels("quote", ticker.Quote).
				Build()

			newKeyCmds = append(newKeyCmds, cmd)
			s.timeSeriesKeys[key] = true
		}
	}

	// 2. Execute New Key Creations (Pipelined)
	if len(newKeyCmds) > 0 {
		// DoMulti executes the commands in a pipeline
		results := s.redisClient.DoMulti(ctx, newKeyCmds...)
		for i, res := range results {
			if err := res.Error(); err != nil {
				// We log but continue, as other keys might be fine
				log.Error("Error lazy-creating time series", "consumer", "redis", "error", err, "index", i)
			}
		}
	}

	// 3. Execute Bulk Insert for Known Keys (Single TS.MADD)
	if len(maddTickers) > 0 {
		maddCmd := s.redisClient.B().TsMadd().KeyTimestampValue()
		for _, ticker := range maddTickers {
			key := fmt.Sprintf("%s:%s:%s:%s", TICKERS_KEY, ticker.Source, ticker.Base, ticker.Quote)
			ts := ticker.Timestamp.UTC().UnixMilli()
			maddCmd = maddCmd.KeyTimestampValue(key, ts, ticker.Price)
		}

		if err := s.redisClient.Do(ctx, maddCmd.Build()).Error(); err != nil {
			log.Error("Error executing TS.MADD", "consumer", "redis", "error", err)
		}
	}
}

func (s *RedisConsumer) runFlushLoop() {
	defer close(s.doneChan)
	tickerInterval := time.NewTicker(FLUSH_INTERVAL)
	defer tickerInterval.Stop()

	for {
		select {
		case <-s.stopChan:
			// Drain remaining buffer on shutdown
			s.flushBuffer()
			return
		case <-tickerInterval.C:
			s.flushBuffer()
		}
	}
}

func (s *RedisConsumer) flushBuffer() {
	s.mutex.Lock()
	if len(s.tickerBuffer) == 0 {
		s.mutex.Unlock()
		return
	}

	// Swap buffers to minimize lock time
	tickersToProcess := s.tickerBuffer
	s.tickerBuffer = make([]*model.Ticker, 0, 500)
	s.mutex.Unlock()

	s.processTickerBatch(tickersToProcess)
}

func (s *RedisConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	log.Debug("Redis ticker listener started", "consumer", "redis")
	s.TickerListener = tickerTopic.Broadcaster.Listen()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("Panic in Redis Consumer listener", "error", r)
			}
		}()

		for t := range s.TickerListener.Channel() {
			ticker := (t.(model.Ticker))

			s.mutex.Lock()
			if len(s.tickerBuffer) >= MAX_BUFFER_CAPACITY {
				s.mutex.Unlock()
				// Use debug/warn depending on how critical dropped frames are
				log.Warn("Redis Consumer buffer full! Dropping ticker.", "base", ticker.Base)
				continue
			}

			s.tickerBuffer = append(s.tickerBuffer, &ticker)
			s.mutex.Unlock()
		}
	}()

	go s.runFlushLoop()
}

func (s *RedisConsumer) CloseTickerListener() {
	// Signal flush loop to stop and drain
	close(s.stopChan)
	<-s.doneChan // Wait for flush to finish

	s.TickerListener.Discard()
	s.redisClient.Close()
	log.Info("Redis Consumer closed gracefully")
}

func NewRedisConsumer(options RedisOptions) *RedisConsumer {
	clientOptions := rueidis.ClientOption{
		Username:    options.ClientOptions.Username,
		Password:    options.ClientOptions.Password,
		InitAddress: options.ClientOptions.InitAddress,
		ClientName:  "ftso-data-provider", // Helps identify connections in Redis
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
		stopChan:          make(chan struct{}),
		doneChan:          make(chan struct{}),
	}
	newConsumer.setup()

	return newConsumer
}
