package consumer

import "github.com/textileio/go-threads/broadcast"

type Consumer interface {
	StartTradeListener(*broadcast.Broadcaster)
	CloseTradeListener()

	StartTickerListener(*broadcast.Broadcaster)
	CloseTickerListener()

	//MessagesInTheLastMinute()
	//MessagesThisPriceEpoch()
}
