package noisy

import (
	"errors"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type NoisySourceOptions struct {
	Interval string `mapstructure:"interval"`
}

type NoisySource struct {
	name         string
	W            *sync.WaitGroup
	TickerTopic  *broadcast.Broadcaster
	Interval     time.Duration
	SymbolList   []model.Symbol
	timeInterval time.Ticker
	log          *slog.Logger
	isRunning    bool
}

func (n *NoisySource) Connect() error {
	n.isRunning = true
	n.SubscribeTickers()
	n.W.Add(1)
	return nil
}

func (n *NoisySource) Reconnect() error {
	return nil
}

func (n *NoisySource) SubscribeTickers() error {
	n.log.Debug("starting fake ticker generation every %d", n.Interval)
	go func(br *broadcast.Broadcaster) {
		n.timeInterval = *time.NewTicker(n.Interval)

		defer n.timeInterval.Stop()

		for t := range n.timeInterval.C {
			randomSymbol := n.SymbolList[rand.Intn(len(n.SymbolList))]
			fakeTicker := model.Ticker{
				LastPrice: strconv.FormatFloat(float64(rand.Intn(1000))+rand.Float64(), 'f', 9, 64),
				Base:      strings.ToUpper(randomSymbol.Base),
				Quote:     strings.ToUpper(randomSymbol.Quote),
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
	if !n.isRunning {
		return errors.New("datasource is not running")
	}
	n.timeInterval.Stop()
	n.isRunning = false
	n.W.Done()

	return nil
}

func (d *NoisySource) IsRunning() bool {
	return d.isRunning
}

func (n *NoisySource) GetName() string {
	return n.name
}

func NewNoisySource(options *NoisySourceOptions, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*NoisySource, error) {
	d, err := time.ParseDuration(options.Interval)
	if err != nil {
		slog.Warn("Using default duration", "datasource", "noisy")
		d = time.Second
	}
	noisy := NoisySource{
		name:        "noisy",
		log:         slog.Default().With(slog.String("datasource", "noisy")),
		Interval:    d,
		W:           w,
		TickerTopic: tickerTopic,
		SymbolList:  symbolList.Flatten(),
	}

	noisy.log.Debug("Created new datasource", "interval", d.String())
	return &noisy, nil
}
