package consumer

import (
	"fmt"
	"io"
	log "log/slog"
	"time"

	"roselabs.mx/ftso-data-sources/model"
)

type IOWriterConsumer struct {
	TradeChannel  <-chan model.Trade
	TickerChannel <-chan model.Ticker

	W io.Writer
}

func (s *IOWriterConsumer) StartTradeListener() {
	// Listen for trades in the ch channel and sends them to a io.Writer
	go func() {
		log.Info("IOWriter consumer listening now")
		for trade := range s.TradeChannel {
			/*jsonStr, err := json.Marshal(trade)
			if err != nil {
				fmt.Printf("Error ")
			} else {
				fmt.Println(jsonStr)
			}*/
			s.W.Write([]byte(fmt.Sprintf(
				"%s source=%s symbol=%s price=%f size=%f side=%s ts=%d\n",
				time.Now().Format("03:04:05"), trade.Source, trade.Symbol, trade.Price, trade.Size, trade.Side, trade.Timestamp.UTC().UnixMilli())))
		}
	}()
}
func (s *IOWriterConsumer) CloseTradeListener() {

}
func (s *IOWriterConsumer) MessagesInTheLastMinute() {

}
func (s *IOWriterConsumer) MessagesThisPriceEpoch() {

}

func NewIOWriterConsumer(w io.Writer, tradeCh <-chan model.Trade) (IOWriterConsumer, error) {
	newConsumer := IOWriterConsumer{
		W:            w,
		TradeChannel: tradeCh,
	}

	return newConsumer, nil
}
