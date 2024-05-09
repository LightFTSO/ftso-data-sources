package noisy

import (
	log "log/slog"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type NoisySourceOptions struct {
	Name     string `mapstructure:"name"`
	Interval string `mapstructure:"interval"`
}

type NoisySource struct {
	name         string
	W            *sync.WaitGroup
	TickerTopic  *broadcast.Broadcaster
	Interval     time.Duration
	SymbolList   []model.Symbol
	timeInterval time.Ticker
}

func (n *NoisySource) Connect() error {
	n.W.Add(1)
	return nil
}

func (n *NoisySource) Reconnect() error {
	return nil
}

func (n *NoisySource) SubscribeTickers() error {
	go func(br *broadcast.Broadcaster) {
		n.timeInterval = *time.NewTicker(n.Interval)

		defer n.timeInterval.Stop()

		for t := range n.timeInterval.C {
			randomSymbol := n.SymbolList[rand.Intn(len(n.SymbolList))]
			fakeTicker := model.Ticker{
				LastPrice: strconv.FormatFloat(float64(rand.Intn(1000))+rand.Float64(), 'f', 9, 64),
				Base:      randomSymbol.Base,
				Quote:     randomSymbol.Quote,
				Symbol:    randomSymbol.GetSymbol(),
				Source:    n.GetName(),
				Timestamp: t,
			}

			br.Send(&fakeTicker)
		}
	}(n.TickerTopic)

	return nil
}

func (n *NoisySource) Close() error {
	n.W.Done()

	return nil
}

func (n *NoisySource) GetName() string {
	return n.name
}

func NewNoisySource(options *NoisySourceOptions, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*NoisySource, error) {
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
		TickerTopic: tickerTopic,
		SymbolList:  symbolList.Flatten(),
	}

	log.Debug("Created new datasource", "datasource", "noisy", "name", noisy.GetName(), "interval", d.String())
	return &noisy, nil
}
