package noisy

import (
	"errors"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type NoisySourceOptions struct {
	Interval string `mapstructure:"interval"`
}

type NoisySource struct {
	name         string
	W            *sync.WaitGroup
	TickerTopic  *tickertopic.TickerTopic
	Interval     time.Duration
	SymbolList   model.SymbolList
	timeInterval *time.Ticker
	log          *slog.Logger
	isRunning    bool
}

func (n *NoisySource) Connect() error {
	n.isRunning = true
	n.SubscribeTickers(nil, nil)
	n.W.Add(1)
	return nil
}

func (n *NoisySource) Reconnect() error {
	return nil
}

func (n *NoisySource) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	n.log.Debug("starting fake ticker generation", "interval", n.Interval.String())
	go func(br *tickertopic.TickerTopic) {
		n.timeInterval = time.NewTicker(n.Interval)

		defer n.timeInterval.Stop()

		for t := range n.timeInterval.C {
			randomSymbol := n.SymbolList[rand.Intn(len(n.SymbolList))]
			fakeTicker, err := model.NewTicker(strconv.FormatFloat(float64(rand.Intn(1000))+rand.Float64(), 'f', 9, 64), randomSymbol, n.GetName(), t)
			if err != nil {
				n.log.Error("Error creating ticker",
					"ticker", fakeTicker, "error", err.Error())
				continue
			}
			br.Send(fakeTicker)
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

func NewNoisySource(options *NoisySourceOptions, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*NoisySource, error) {
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
