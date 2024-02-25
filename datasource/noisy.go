package datasource

import (
	"fmt"
	log "log/slog"
	"math/rand"
	"sync"
	"time"

	"roselabs.mx/ftso-data-sources/model"
)

type NoisySource struct {
	Name       string
	timeTicker time.Ticker
	Duration   time.Duration
	W          *sync.WaitGroup
	TradeChan  *chan model.Trade
}

func (n *NoisySource) StartTrades() error {
	log.Info(fmt.Sprintf("Creating new noisy source \"%s\"", n.Name))
	n.W.Add(1)

	go func(ch chan<- model.Trade) {
		n.timeTicker = *time.NewTicker(n.Duration)

		defer n.timeTicker.Stop()

		for t := range n.timeTicker.C {
			fakeTrade := model.Trade{
				Base:      "ABC",
				Quote:     "XYZ",
				Symbol:    "ABC/XYZ",
				Price:     float64(rand.Intn(1000)) + rand.Float64(),
				Size:      float64(rand.Intn(1000)) + rand.Float64(),
				Side:      "WHAT? LMAO",
				Source:    n.Name,
				Timestamp: t,
			}

			ch <- fakeTrade
		}
	}(*n.TradeChan)

	return nil

}

func (n *NoisySource) Subscribe(baseList []string, quoteList []string) ([]string, error) {
	return []string{}, nil

}

func (n *NoisySource) Close() {
	n.W.Done()

}

func NewNoisySource(name string, d time.Duration, tradeChan *chan model.Trade, w *sync.WaitGroup) NoisySource {
	noisy := NoisySource{
		Name:      name,
		Duration:  d,
		W:         w,
		TradeChan: tradeChan,
	}

	return noisy
}
