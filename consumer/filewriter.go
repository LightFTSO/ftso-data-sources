package consumer

import (
	"fmt"
	log "log/slog"
	"os"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
)

type FileConsumerOptions struct {
	Enabled        bool
	OutputFilename string `mapstructure:"filename"`
}

type FileConsumer struct {
	TradeListener  *broadcast.Listener
	TickerListener *broadcast.Listener

	W *os.File
}

func (s *FileConsumer) processTrade(trade *model.Trade) {
	s.W.Write([]byte(fmt.Sprintf(
		"%s source=%s symbol=%s price=%s size=%s side=%s ts=%d\n",
		time.Now().Format(constants.TS_FORMAT), trade.Source, trade.Symbol, trade.Price, trade.Size, trade.Side, trade.Timestamp.UTC().UnixMilli())))
}

// Listen for trades in the ch channel and sends them to a io.Writer
func (s *FileConsumer) StartTradeListener(tradeTopic *broadcast.Broadcaster) {
	s.TradeListener = tradeTopic.Listen()
	go func() {
		log.Info("IOWriter consumer listening now")
		for trade := range s.TradeListener.Channel() {
			s.processTrade(trade.(*model.Trade))
		}

		s.W.Close()
	}()

}

func (s *FileConsumer) CloseTradeListener() {

}

func (s *FileConsumer) processTicker(ticker *model.Ticker) {
	s.W.Write([]byte(fmt.Sprintf(
		"%s source=%s symbol=%s last_price=%s ts=%d\n",
		time.Now().Format(constants.TS_FORMAT), ticker.Source, ticker.Symbol, ticker.LastPrice, ticker.Timestamp.UTC().UnixMilli())))
}

// Listen for trades in the ch channel and sends them to a io.Writer
func (s *FileConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	s.TickerListener = tickerTopic.Listen()
	go func() {
		log.Info("IOWriter consumer listening now")
		for ticker := range s.TradeListener.Channel() {
			s.processTicker(ticker.(*model.Ticker))
		}

		s.W.Close()
	}()

}
func (s *FileConsumer) CloseTickerListener() {

}

func NewFileConsumer(filename string) *FileConsumer {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	newConsumer := &FileConsumer{
		W: file,
	}
	return newConsumer
}
