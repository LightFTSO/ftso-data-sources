package consumer

import "roselabs.mx/ftso-data-sources/model"

type QuestDbConsumer struct {
	TradeChannel  <-chan model.Trade
	TickerChannel <-chan model.Ticker
}
