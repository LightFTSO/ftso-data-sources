package consumer

import (
	"github.com/textileio/go-threads/broadcast"
)

type Consumer interface {
	StartTickerListener(*broadcast.Broadcaster)
	CloseTickerListener()
	//processTicker(ticker *model.Ticker)
}
