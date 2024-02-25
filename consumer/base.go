package consumer

import "roselabs.mx/ftso-data-sources/model"

type Consumer interface {
	StartTradeListener(<-chan model.Trade)
	CloseTradeListener()

	StartTickerListener(<-chan model.Ticker)
	CloseTickerListener()

	MessagesInTheLastMinute()
	MessagesThisPriceEpoch()
}
