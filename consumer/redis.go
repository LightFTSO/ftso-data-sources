package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
)

type RedisConsumer struct {
	TradeListener  *broadcast.Listener
	TickerListener *broadcast.Listener

	toStdout bool

	numThreads int

	redisClient rueidis.Client

	tsRetention time.Duration
	tsChunkSize int64
}

type RedisOptions struct {
	Enabled       bool
	ClientOptions rueidis.ClientOption `mapstructure:"client_options"`
	NumThreads    int                  `mapstructure:"num_threads"`
	IncludeStdout bool                 `mapstructure:"include_stdout"`
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

func (s *RedisConsumer) processTrade(trade *model.Trade) {
	if s.toStdout {
		fmt.Printf(
			"%s source=%s symbol=%s price=%s size=%s side=%s ts=%d\n",
			time.Now().Format(constants.TS_FORMAT), trade.Source, trade.Symbol, trade.Price, trade.Size, trade.Side, trade.Timestamp.UTC().UnixMilli())
	}
	key := fmt.Sprintf("ts:%s:%s:trade_prices", trade.Source, trade.Symbol)
	val, _ := strconv.ParseFloat(trade.Price, 64)
	cmd := s.redisClient.B().TsAdd().Key(key).Timestamp(strconv.FormatInt(trade.Timestamp.UTC().UnixMilli(), 10)).Value(val).Retention(s.tsRetention.Milliseconds()).EncodingCompressed().OnDuplicateLast().Labels().Labels("type", "trade_prices").Labels("source", trade.Source).Labels("base", trade.Base).Labels("quote", trade.Quote).Build()
	err := s.redisClient.Do(context.Background(), cmd).Error()
	if err != nil {
		log.Error("Error executing ts.ADD", "consumer", "redis", "error", err)
	}
}

func (s *RedisConsumer) StartTradeListener(tradeTopic *broadcast.Broadcaster) {
	// Listen for trades in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Redis trade listener configured with %d consumer goroutines", s.numThreads), "consumer", "redis", "num_threads", s.numThreads)
	s.TradeListener = tradeTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Redis trade consumer %d listening for trades now", consumerId), "consumer", "redis", "consumer_num", consumerId)
			for trade := range s.TradeListener.Channel() {
				s.processTrade(trade.(*model.Trade))
			}
		}(consumerId)
	}

}
func (s *RedisConsumer) CloseTradeListener() {
	s.TradeListener.Discard()
}

func (s *RedisConsumer) processTicker(ticker *model.Ticker) {
	if s.toStdout {
		fmt.Printf(
			"%s source=%s symbol=%s last_price=%s ts=%d\n",
			time.Now().Format(constants.TS_FORMAT), ticker.Source, ticker.Symbol, ticker.LastPrice, ticker.Timestamp.UTC().UnixMilli())
	}
	key := fmt.Sprintf("ts:%s:%s:ticker_prices", ticker.Source, ticker.Symbol)
	val, _ := strconv.ParseFloat(ticker.LastPrice, 64)
	cmd := s.redisClient.B().TsAdd().Key(key).Timestamp(strconv.FormatInt(ticker.Timestamp.UTC().UnixMilli(), 10)).Value(val).Retention(s.tsRetention.Milliseconds()).EncodingCompressed().OnDuplicateLast().Labels().Labels("type", "ticker_prices").Labels("source", ticker.Source).Labels("base", ticker.Base).Labels("quote", ticker.Quote).Build()
	err := s.redisClient.Do(context.Background(), cmd).Error()
	if err != nil {
		log.Error("Error executing ts.ADD", "consumer", "redis", "error", err)
	}
}

func (s *RedisConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for trades in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Redis trade listener configured with %d consumer goroutines", s.numThreads), "consumer", "redis", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Redis ticker consumer %d listening for tickers now", consumerId), "consumer", "redis", "consumer_num", consumerId)
			for ticker := range s.TradeListener.Channel() {
				s.processTicker(ticker.(*model.Ticker))
			}
		}(consumerId)
	}

}
func (s *RedisConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewRedisConsumer(options RedisOptions) *RedisConsumer {
	r, err := rueidis.NewClient(options.ClientOptions)
	if err != nil {
		panic(err)
	}

	newConsumer := &RedisConsumer{
		redisClient: r,
		numThreads:  options.NumThreads,
		toStdout:    options.IncludeStdout,
		tsRetention: options.TsOptions.Retention,
		tsChunkSize: options.TsOptions.ChunkSize,
	}
	newConsumer.setup()

	return newConsumer
}
