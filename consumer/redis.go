package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type RedisConsumer struct {
	TickerListener *broadcast.Listener

	numThreads int

	redisClient rueidis.Client

	tsRetention time.Duration
	tsChunkSize int64
}

type RedisOptions struct {
	Enabled       bool
	ClientOptions rueidis.ClientOption `mapstructure:"client_options"`
	NumThreads    int                  `mapstructure:"num_threads"`
	TsOptions     struct {
		Retention time.Duration
		ChunkSize int64
	} `mapstructure:"ts"`
}

func (s *RedisConsumer) setup() error {
	log.Info("Creating informational keys", "consumer", "redis")
	log.Info("Setting up regular info messages", "consumer", "redis")
	cmd := s.redisClient.B().Keys().Pattern("ts:*").Build()
	tsKeys, err := s.redisClient.Do(context.Background(), cmd).AsStrSlice()
	if err != nil {
		log.Error("Error creating meta informational keys", "consumer", "redis")
		return err
	}

	log.Info("Updating retention rules", "consumer", "redis")
	for _, key := range tsKeys {
		cmd := s.redisClient.B().TsAlter().Key(key).Retention(s.tsRetention.Milliseconds()).ChunkSize(s.tsChunkSize).Build()
		s.redisClient.Do(context.Background(), cmd)
	}

	return nil

}

func (s *RedisConsumer) processTicker(ticker *model.Ticker) {
	key := fmt.Sprintf("ts:%s:%s", ticker.Source, ticker.Symbol())
	val, _ := strconv.ParseFloat(ticker.LastPrice, 64)
	cmd := s.redisClient.B().TsAdd().Key(key).Timestamp(strconv.FormatInt(ticker.Timestamp.UTC().UnixMilli(), 10)).
		Value(val).Retention(s.tsRetention.Milliseconds()).EncodingCompressed().OnDuplicateLast().Labels().Labels("type", "ticker").
		Labels("source", ticker.Source).Labels("base", ticker.Base).Labels("quote", ticker.Quote).Build()
	err := s.redisClient.Do(context.Background(), cmd).Error()
	if err != nil {
		log.Error("Error executing ts.ADD", "consumer", "redis", "error", err)
	}
}

func (s *RedisConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	// Listen for tickers in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Redis ticker listener configured with %d consumer goroutines", s.numThreads), "consumer", "redis", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Redis ticker consumer %d listening for tickers now", consumerId), "consumer", "redis", "consumer_num", consumerId)
			for ticker := range s.TickerListener.Channel() {
				s.processTicker(ticker.(*model.Ticker))
			}
		}(consumerId)
	}

}
func (s *RedisConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
	s.redisClient.Close()
}

func NewRedisConsumer(options RedisOptions) *RedisConsumer {
	r, err := rueidis.NewClient(options.ClientOptions)
	if err != nil {
		panic(err)
	}

	newConsumer := &RedisConsumer{
		redisClient: r,
		numThreads:  options.NumThreads,
		tsRetention: options.TsOptions.Retention,
		tsChunkSize: options.TsOptions.ChunkSize,
	}
	newConsumer.setup()

	return newConsumer
}
