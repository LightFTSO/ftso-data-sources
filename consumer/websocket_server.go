package consumer

import "roselabs.mx/ftso-data-sources/model"

type WebsocketServerConsumerOptions struct {
	Enabled   bool
	Host      string
	Port      uint
	Endpoints struct {
		Trades  string
		Tickers string
		Volumes string
	}
	UseSbeEncoding bool `mapstructure:"use_sbe_encoding"`
}

type WebsocketServerConsumer struct {
	TradeChannel  <-chan model.Trade
	TickerChannel <-chan model.Ticker
}

func (s *WebsocketServerConsumer) StartTradeListener(tradeChan <-chan model.Trade) {
	s.TradeChannel = tradeChan

}

func (s *WebsocketServerConsumer) CloseTradeListener() {

}
func (s *WebsocketServerConsumer) StartTickerListener(tickerChan <-chan model.Ticker) {
	s.TickerChannel = tickerChan

}
func (s *WebsocketServerConsumer) CloseTickerListener() {

}

func (s *WebsocketServerConsumer) MessagesInTheLastMinute() {

}
func (s *WebsocketServerConsumer) MessagesThisPriceEpoch() {

}
