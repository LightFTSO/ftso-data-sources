package noisy

import (
	log "log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
)

type NoisySourceOptions struct {
	Name     string `mapstructure:"name"`
	Interval string `mapstructure:"interval"`
}

type NoisySource struct {
	name        string
	W           *sync.WaitGroup
	TradeTopic  *broadcast.Broadcaster
	TickerTopic *broadcast.Broadcaster
	Interval    time.Duration
}

func (n *NoisySource) Connect() error {
	n.W.Add(1)
	return nil
}

func (n *NoisySource) Reconnect() error {
	return nil
}

func (n *NoisySource) SubscribeTrades() error {
	go func(br *broadcast.Broadcaster) {
		timeInterval := *time.NewTicker(n.Interval)

		defer timeInterval.Stop()

		for t := range timeInterval.C {
			fakeTrade := model.Trade{
				Base:      "ABC",
				Quote:     "XYZ",
				Symbol:    "ABC/XYZ",
				Price:     float64(rand.Intn(1000)) + rand.Float64(),
				Size:      float64(rand.Intn(1000)) + rand.Float64(),
				Side:      "WHAT? LMAO",
				Source:    n.GetName(),
				Timestamp: t,
			}

			br.Send(&fakeTrade)
		}
	}(n.TradeTopic)

	return nil
}

func (n *NoisySource) SubscribeTickers() error {
	go func(br *broadcast.Broadcaster) {
		timeInterval := *time.NewTicker(n.Interval)

		defer timeInterval.Stop()

		for t := range timeInterval.C {
			fakeTicker := model.Ticker{
				LastPrice: float64(rand.Intn(1000)) + rand.Float64(),
				Base:      "ABC",
				Quote:     "XYZ",
				Symbol:    "ABC/XYZ",
				Source:    n.GetName(),
				Timestamp: t,
			}

			br.Send(&fakeTicker)
		}
	}(n.TradeTopic)

	return nil
}

func (n *NoisySource) Close() error {
	n.W.Done()

	return nil
}

func (n *NoisySource) GetName() string {
	return n.name
}

func NewNoisySource(options *NoisySourceOptions, tradeTopic *broadcast.Broadcaster, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*NoisySource, error) {
	d, err := time.ParseDuration(options.Interval)
	if err != nil {
		log.Info("Using default duration", "datasource", "noisy", "name", options.Name)
		d = time.Second
	}
	if options.Name == "" {
		options.Name = "noisy" + d.String()
	}
	noisy := NoisySource{
		name:        options.Name,
		Interval:    d,
		W:           w,
		TradeTopic:  tradeTopic,
		TickerTopic: tickerTopic,
	}

	log.Info("Created new datasource", "datasource", "noisy", "name", options.Name, "interval", d.String())
	return &noisy, nil
}
