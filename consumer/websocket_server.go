package consumer

import "roselabs.mx/ftso-data-sources/model"

type WebsocketServerConsumerOptions struct {
	Enabled   bool
	Host      string
	Port      uint
	Endpoints struct {
		Tickers string
		Volumes string
	}
	UseSbeEncoding bool `mapstructure:"use_sbe_encoding"`
}

type WebsocketServerConsumer struct {
	TickerChannel <-chan model.Ticker
}

func (s *WebsocketServerConsumer) StartTickerListener(tickerChan <-chan model.Ticker) {
	s.TickerChannel = tickerChan

}
func (s *WebsocketServerConsumer) CloseTickerListener() {

}
