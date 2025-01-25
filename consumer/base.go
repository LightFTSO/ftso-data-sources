package consumer

import "roselabs.mx/ftso-data-sources/tickertopic"

type Consumer interface {
	StartTickerListener(*tickertopic.TickerTopic)
	CloseTickerListener()
}
