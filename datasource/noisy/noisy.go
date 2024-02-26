package noisy

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
	W          *sync.WaitGroup
	TradeChan  *chan model.Trade
	timeTicker time.Ticker
	Duration   time.Duration
}

func (n *NoisySource) Connect() error {
	n.W.Add(1)
	log.Info(fmt.Sprintf("Creating new noisy source \"%s\"", n.Name))
	return nil
}

func (n *NoisySource) Reconnect() error {
	return nil
}

func (n *NoisySource) StartTrades() error {

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

func (n *NoisySource) SubscribeTrades() error {
	return nil

}

func (n *NoisySource) Close() error {
	n.W.Done()

	return nil
}

func (b *NoisySource) GetName() string {
	return b.Name
}

func NewNoisySource(name string, d time.Duration, tradeChan *chan model.Trade, w *sync.WaitGroup) *NoisySource {
	log.Info("Created new datasource", "datasource", "noisy")
	noisy := NoisySource{
		Name:      name,
		Duration:  d,
		W:         w,
		TradeChan: tradeChan,
	}

	return &noisy
}
