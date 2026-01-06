package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

// Configurable Buckets for Histogram (Upper bounds in ms)
var LATENCY_BUCKETS = []int64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000}

type StatisticsGeneratorOptions struct {
	Enabled  bool          `mapstructure:"enabled"`
	Interval time.Duration `mapstructure:"interval"`
}

type StatisticsGenerator struct {
	enabled        bool
	TickerListener *broadcast.Listener
	statsInterval  time.Duration

	mu sync.Mutex

	// Current Interval Counters
	tickerCount      uint64
	sourceCounts     map[string]uint64
	sourceLatencySum map[string]time.Duration
	latencySum       time.Duration
	latMin           time.Duration
	latMax           time.Duration
	futureTSCount    uint64
	worstFutureSkew  time.Duration
	latBuckets       []uint64

	// History for Trend Analysis
	prevTPS float64

	// State tracking
	lastReportTime time.Time
	lastTickTime   time.Time

	// Lifecycle
	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup
}

func NewStatisticsGenerator(options StatisticsGeneratorOptions) *StatisticsGenerator {
	if options.Interval <= 0 {
		options.Interval = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &StatisticsGenerator{
		enabled:          options.Enabled,
		statsInterval:    options.Interval,
		sourceCounts:     make(map[string]uint64),
		sourceLatencySum: make(map[string]time.Duration), // Initialize map
		latBuckets:       make([]uint64, len(LATENCY_BUCKETS)+1),
		ctx:              ctx,
		cancel:           cancel,
	}
}

func (s *StatisticsGenerator) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	if !s.enabled {
		log.Info("Ticker stats disabled", "consumer", "statistics")
		return
	}

	log.Debug("Ticker Statistics generator started", "consumer", "statistics")
	s.TickerListener = tickerTopic.Broadcaster.Listen()

	s.mu.Lock()
	s.lastReportTime = time.Now()
	s.mu.Unlock()

	// 1. Start Reporting Loop
	s.wg.Add(1)
	go s.runReportingLoop()

	// 2. Start Ingestion Loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.TickerListener.Discard()

		log.Debug("Ticker statistics generator listening now", "consumer", "statistics")

		for {
			select {
			case <-s.ctx.Done():
				return
			case t, ok := <-s.TickerListener.Channel():
				if !ok {
					return
				}
				// Safe Type Assertion
				ticker, ok := t.(model.Ticker)
				if !ok {
					continue
				}

				s.recordMetric(ticker)
			}
		}
	}()
}

func (s *StatisticsGenerator) recordMetric(ticker model.Ticker) {
	now := time.Now()
	lag := now.Sub(ticker.Timestamp)
	skew := time.Duration(0)

	// Handle Future Timestamps (Clock Skew)
	if lag < 0 {
		skew = -lag
		lag = 0
	}

	s.mu.Lock()

	// Counters
	s.tickerCount++
	s.sourceCounts[ticker.Source]++
	s.sourceLatencySum[ticker.Source] += lag // NEW: Add to specific source sum
	s.latencySum += lag
	s.lastTickTime = now

	// Min/Max Latency
	if s.tickerCount == 1 {
		s.latMin, s.latMax = lag, lag
	} else {
		if lag < s.latMin {
			s.latMin = lag
		}
		if lag > s.latMax {
			s.latMax = lag
		}
	}

	// Clock Skew Tracking
	if skew > 0 {
		s.futureTSCount++
		if skew > s.worstFutureSkew {
			s.worstFutureSkew = skew
		}
	}

	// Histogram
	bucketFound := false
	ms := lag.Milliseconds()
	for i, edge := range LATENCY_BUCKETS {
		if ms < edge {
			s.latBuckets[i]++
			bucketFound = true
			break
		}
	}
	if !bucketFound {
		s.latBuckets[len(LATENCY_BUCKETS)]++
	}

	s.mu.Unlock()
}

func (s *StatisticsGenerator) CloseTickerListener() {
	s.cancel()
	s.wg.Wait()
}

func (s *StatisticsGenerator) runReportingLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.statsInterval)
	defer t.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.printAndResetStats()
		}
	}
}

func (s *StatisticsGenerator) printAndResetStats() {
	s.mu.Lock()

	// 1. Snapshot Data
	count := s.tickerCount
	totalLatency := s.latencySum
	latMin, latMax := s.latMin, s.latMax
	futureCount, worstSkew := s.futureTSCount, s.worstFutureSkew

	sources := make(map[string]uint64, len(s.sourceCounts))
	for k, v := range s.sourceCounts {
		sources[k] = v
	}

	sourceLatSums := make(map[string]time.Duration, len(s.sourceLatencySum))
	for k, v := range s.sourceLatencySum {
		sourceLatSums[k] = v
	}

	buckets := make([]uint64, len(s.latBuckets))
	copy(buckets, s.latBuckets)

	// Calculate precise duration
	now := time.Now()
	duration := now.Sub(s.lastReportTime).Seconds()
	if duration <= 0 {
		duration = 1
	}
	s.lastReportTime = now
	lastTick := s.lastTickTime

	// 2. Reset Counters
	s.tickerCount = 0
	s.latencySum = 0
	s.latMin, s.latMax = 0, 0
	s.futureTSCount = 0
	s.worstFutureSkew = 0
	for i := range s.latBuckets {
		s.latBuckets[i] = 0
	}
	// Re-make maps
	s.sourceCounts = make(map[string]uint64)
	s.sourceLatencySum = make(map[string]time.Duration) // Reset

	s.mu.Unlock()

	// 3. Logic & Anomaly Detection

	// A. Silence Detection
	if count == 0 {
		ageStr := "n/a"
		if !lastTick.IsZero() {
			ageStr = fmt.Sprintf("%.0fs", time.Since(lastTick).Seconds())
		}
		log.Warn("Stats: NO DATA received", "duration", fmt.Sprintf("%.1fs", duration), "last_seen", ageStr)
		s.prevTPS = 0
		return
	}

	tps := float64(count) / duration
	avgLagMs := float64(totalLatency.Milliseconds()) / float64(count)

	// B. Trend Analysis (TPS Drop Warning)
	// If TPS drops by > 50% compared to last interval (and prev wasn't zero)
	if s.prevTPS > 0 && tps < (s.prevTPS*0.5) {
		log.Warn("Significant TPS Drop detected", "prev_tps", fmt.Sprintf("%.1f", s.prevTPS), "curr_tps", fmt.Sprintf("%.1f", tps))
	}
	s.prevTPS = tps

	// C. Clock Skew Warning
	if worstSkew > 5*time.Second {
		log.Error("Critical Clock Skew or Future Timestamp detected", "max_skew", worstSkew.String())
	}

	// D. Build detailed Exchange Stats - NEW
	// Creates a map like: "Binance": {"count": 100, "avg_ms": 45}
	exchangeDetails := make(map[string]map[string]any)
	for src, cnt := range sources {
		sumLat := sourceLatSums[src]
		avg := float64(sumLat.Milliseconds()) / float64(cnt)
		exchangeDetails[src] = map[string]any{
			"count":  cnt,
			"lag_ms": int64(avg),
		}
	}

	// 4. Structured Logging
	log.Info("Ticker Stats",
		log.Group("throughput",
			log.String("tps", fmt.Sprintf("%.1f", tps)),
			log.Uint64("total", count),
		),
		log.Group("latency",
			log.Float64("avg_ms", avgLagMs),
			log.Int64("min_ms", latMin.Milliseconds()),
			log.Int64("max_ms", latMax.Milliseconds()),
			log.Int64("p95_ms", calculatePercentile(buckets, count, 0.95)),
		),
		log.Group("health",
			log.Uint64("future_ts_count", futureCount),
			log.String("max_skew", worstSkew.String()),
			log.String("since_last_tick", time.Since(lastTick).Round(time.Millisecond).String()),
		),
		log.Any("exchanges", exchangeDetails), // Replaced "sources" with detailed stats
	)
}

// Returns the Upper Bound of the bucket where the percentile falls
func calculatePercentile(buckets []uint64, total uint64, p float64) int64 {
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * p)
	var accumulated uint64

	for i, count := range buckets {
		accumulated += count
		if accumulated >= target {
			if i < len(LATENCY_BUCKETS) {
				return LATENCY_BUCKETS[i]
			}
			return LATENCY_BUCKETS[len(LATENCY_BUCKETS)-1] + 1
		}
	}
	return 0
}
