package consumer

import (
	"fmt"
	"io"
	log "log/slog"
	"time"

	"roselabs.mx/ftso-data-sources/model"
)

type Stdout struct {
	TradeChannel  <-chan model.Trade
	TickerChannel <-chan model.Ticker

	W io.Writer
}

func (s *Stdout) StartTradeListener() {
	// Listen for trades in the ch channel and sends them to a io.Writer
	go func() {
		log.Info("Listening now")
		for trade := range s.TradeChannel {
			/*jsonStr, err := json.Marshal(trade)
			if err != nil {
				fmt.Printf("Error ")
			} else {
				fmt.Println(jsonStr)
			}*/
			s.W.Write([]byte(fmt.Sprintf(
				"%s source=%s symbol=%s price=%0.5f size=%0.5f side=%s ts=%d\n",
				time.Now().Format("2006/01/02 03:04:05"), trade.Source, trade.Symbol, trade.Price, trade.Size, trade.Side, trade.Timestamp.UTC().UnixMilli())))
		}
	}()
}
func (s *Stdout) CloseTradeListener() {

}
func (s *Stdout) MessagesInTheLastMinute() {

}
func (s *Stdout) MessagesThisPriceEpoch() {

}

func NewIOWriterConsumer(w io.Writer, tradeCh <-chan model.Trade) (Stdout, error) {
	newConsumer := Stdout{
		W:            w,
		TradeChannel: tradeCh,
	}

	return newConsumer, nil
}
