package consumer

import (
	"fmt"
	log "log/slog"
	"sync/atomic"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/tickertopic"
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

func (s *StatisticsGenerator) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	log.Debug(fmt.Sprintf("Ticker Statistics generator configured with %d consumer goroutines", s.numThreads), "consumer", "statistics", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Ticker statistics generator %d listening now", consumerId), "consumer", "statistics", "consumer_num", consumerId)
			for range s.TickerListener.Channel() {
				s.tickerCounter.Add(1)
			}
		}(consumerId)
	}

}
func (s *StatisticsGenerator) CloseTickerListener() {

}

func (s *StatisticsGenerator) MessagesInTheLastInterval() {
	go func() {
		startTs := time.Now()
		// wait enough time to be aligned with the start of the next minute
		time.Sleep((time.Duration(60-startTs.Second()-1) * time.Second) +
			(time.Duration((1e9 - startTs.Nanosecond())) * time.Nanosecond))
		s.printTickerCount(startTs)

		timeTicker := time.NewTicker(s.statsInterval)

		defer timeTicker.Stop()

		for range timeTicker.C {
			s.printTickerCount(time.Now().Add(-s.statsInterval))
		}
	}()
}

func (s *StatisticsGenerator) printTickerCount(startTime time.Time) {
	runningTime := time.Since(startTime)

	totalTickers := s.tickerCounter.Swap(0)
	tickersPerSecond := float64(totalTickers) / runningTime.Seconds()
	log.Info(fmt.Sprintf("Received %d tickers in the last %.0f seconds %.1f tickers/s", totalTickers, runningTime.Seconds(), tickersPerSecond))
}

func (s *StatisticsGenerator) MessagesThisPriceEpoch() {

}

func NewStatisticsGenerator(options StatisticsGeneratorOptions) *StatisticsGenerator {
	newConsumer := &StatisticsGenerator{
		numThreads:    options.NumThreads,
		statsInterval: options.Interval, //10 * time.Second,
	}
	newConsumer.MessagesInTheLastInterval()
	return newConsumer
}
