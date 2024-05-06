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
	TickerListener *broadcast.Listener

	numThreads int

	tickerCounter atomic.Uint64

	statsInterval time.Duration

	Statistics map[string]interface{}
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
		startTs := time.Now()
		time.Sleep(time.Duration(60-startTs.Truncate(time.Second).Second()) * time.Second)
		s.printTickerCount(startTs)

		timeTicker := time.NewTicker(s.statsInterval)

		defer timeTicker.Stop()

		for range timeTicker.C {
			s.printTickerCount(time.Now().Add(-s.statsInterval))
		}
	}()

}

func (s *StatisticsGenerator) printTickerCount(startTime time.Time) {
	if s.TickerListener != nil {
		runningTime := time.Since(startTime)

		totalTickers := s.tickerCounter.Swap(0)
		tickersPerSecond := float64(totalTickers) / runningTime.Seconds()
		log.Info(fmt.Sprintf("Received %d tickers in the last %.0f seconds %.1f tickers/s", totalTickers, runningTime.Seconds(), tickersPerSecond))
	}
}

func (s *StatisticsGenerator) MessagesThisPriceEpoch() {

}

func NewStatisticsGenerator(options StatisticsGeneratorOptions) *StatisticsGenerator {
	newConsumer := &StatisticsGenerator{
		numThreads:    options.NumThreads,
		statsInterval: options.Interval, //10 * time.Second,
	}
	newConsumer.MessagesInTheLastMinute()
	return newConsumer
}
