package noisy

import (
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/textileio/go-threads/broadcast"

	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type NoisySourceOptions struct {
	Interval string `mapstructure:"interval"`
}

type NoisySource struct {
	name             string
	W                *sync.WaitGroup
	TickerTopic      *tickertopic.TickerTopic
	Interval         time.Duration
	SymbolList       model.SymbolList
	timeInterval     *time.Ticker
	log              *slog.Logger
	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func (d *NoisySource) Connect() error {
	d.isRunning = true
	d.SubscribeTickers(nil, nil)
	d.W.Add(1)
	return nil
}

func (d *NoisySource) Reconnect() error {
	return nil
}

func (d *NoisySource) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	d.log.Debug("starting fake ticker generation", "interval", d.Interval.String())
	go func(br *tickertopic.TickerTopic) {
		d.timeInterval = time.NewTicker(d.Interval)

		defer d.timeInterval.Stop()

		for t := range d.timeInterval.C {
			randomSymbol := d.SymbolList[rand.Intn(len(d.SymbolList))]
			fakeTicker, err := model.NewTicker(float64(rand.Intn(1000))+rand.Float64(), randomSymbol, d.GetName(), t)
			if err != nil {
				d.log.Error("Error creating ticker",
					"ticker", fakeTicker, "error", err.Error())
				continue
			}
			br.Send(fakeTicker)
		}
	}(d.TickerTopic)

	return nil
}

func (d *NoisySource) Close() error {
	if !d.isRunning {
		return errors.New("datasource is not running")
	}
	d.timeInterval.Stop()
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.clientClosedChan.Send(true)
	d.W.Done()

	return nil
}

func (d *NoisySource) IsRunning() bool {
	return d.isRunning
}

func (d *NoisySource) GetName() string {
	return d.name
}

func NewNoisySource(options *NoisySourceOptions, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*NoisySource, error) {
	d, err := time.ParseDuration(options.Interval)
	if err != nil {
		slog.Warn("Using default duration", "datasource", "noisy")
		d = time.Second
	}
	noisy := NoisySource{
		name:             "noisy",
		log:              slog.Default().With(slog.String("datasource", "noisy")),
		Interval:         d,
		W:                w,
		TickerTopic:      tickerTopic,
		SymbolList:       symbolList.Flatten(),
		clientClosedChan: broadcast.NewBroadcaster(0),
	}

	noisy.log.Debug("Created new datasource", "interval", d.String())
	return &noisy, nil
}
