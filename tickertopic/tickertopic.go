package tickertopic

import (
	"fmt"
	"log/slog"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
)

type TickerTopic struct {
	Broadcaster *broadcast.Broadcaster

	transformations []Transformation
}

func NewTickerTopic(transformationOptions []TransformationOptions, capacity int) *TickerTopic {
	tickerBroadcaster := broadcast.NewBroadcaster(capacity)

	transformations, err := createTransformations(transformationOptions)
	if err != nil {
		slog.Error("An error occurred while creating ticker transformations, program will now exit", "error", err)
		// TODO: Add trace info here
		panic(err)
	}

	tickerTopic := TickerTopic{
		Broadcaster:     tickerBroadcaster,
		transformations: transformations,
	}

	return &tickerTopic
}

func (t *TickerTopic) Send(ticker *model.Ticker) {
	fmt.Printf("ticker before: %+v\n", ticker)
	ticker = t.applyTransformations(ticker)
	fmt.Printf("ticker  after: %+v\n", ticker)

	t.Broadcaster.Send(ticker)
}

func (t *TickerTopic) applyTransformations(ticker *model.Ticker) *model.Ticker {
	if len(t.transformations) <= 0 {
		return ticker
	}

	for _, v := range t.transformations {
		err := v.Transform(ticker)
		if err != nil {
			return nil
		}
	}

	return ticker
}
