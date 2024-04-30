package consumer

import "roselabs.mx/ftso-data-sources/model"

type QuestDbConsumer struct {
	TickerChannel <-chan model.Ticker
}
