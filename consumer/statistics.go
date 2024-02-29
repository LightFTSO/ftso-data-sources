package consumer

import (
	"fmt"
	log "log/slog"
	"sync/atomic"
	"time"

	"github.com/textileio/go-threads/broadcast"
)

type StatisticsGeneratorOptions struct {
	Enabled    bool          `mapstructure:"enabled"`
	Interval   time.Duration `mapstructure:"interval"`
	NumThreads int           `mapstructure:"num_threads"`
}

type StatisticsGenerator struct {
	TradeListener  *broadcast.Listener
	TickerListener *broadcast.Listener

	numThreads int

	tradeCounter  atomic.Uint64
	tickerCounter atomic.Uint64

	statsInterval time.Duration

	Statistics map[string]interface{}
}

func (s *StatisticsGenerator) StartTradeListener(tradeTopic *broadcast.Broadcaster) {
	log.Debug(fmt.Sprintf("Trade Statistics generator configured with %d consumer goroutines", s.numThreads), "consumer", "statistics", "num_threads", s.numThreads)
	s.TradeListener = tradeTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Trade statistics generator %d listening now", consumerId), "consumer", "statistics", "consumer_num", consumerId)
			for range s.TradeListener.Channel() {
				s.tradeCounter.Add(1)
			}
		}(consumerId)
	}
}
func (s *StatisticsGenerator) CloseTradeListener() {

}
func (s *StatisticsGenerator) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	log.Debug(fmt.Sprintf("Ticker Statistics generator configured with %d consumer goroutines", s.numThreads), "consumer", "statistics", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			for range s.TickerListener.Channel() {
				log.Debug(fmt.Sprintf("Ticker statistics generator %d listening now", consumerId), "consumer", "statistics", "consumer_num", consumerId)
				s.tickerCounter.Add(1)
			}
		}(consumerId)
	}

}
func (s *StatisticsGenerator) CloseTickerListener() {

}

func (s *StatisticsGenerator) MessagesInTheLastMinute() {
	go func() {
		timeTicker := time.NewTicker(s.statsInterval)

		defer timeTicker.Stop()

		for range timeTicker.C {
			if s.TradeListener != nil {
				log.Info(fmt.Sprintf("Received %d trades in the last %v seconds", s.tradeCounter.Swap(0), s.statsInterval.Seconds()))
			}
			if s.TickerListener != nil {
				log.Info(fmt.Sprintf("Received %d tickers in the last %v seconds", s.tradeCounter.Swap(0), s.statsInterval.Seconds()))
			}
		}
	}()

}
func (s *StatisticsGenerator) MessagesThisPriceEpoch() {

}

func NewStatisticsGenerator(options StatisticsGeneratorOptions) *StatisticsGenerator {
	newConsumer := &StatisticsGenerator{
		numThreads:    options.NumThreads,
		statsInterval: options.Interval,
	}
	newConsumer.MessagesInTheLastMinute()
	return newConsumer
}
