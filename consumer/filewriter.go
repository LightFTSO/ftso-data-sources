package consumer

import (
	"fmt"
	log "log/slog"
	"os"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type FileConsumerOptions struct {
	Enabled        bool
	OutputFilename string `mapstructure:"filename"`
}

type FileConsumer struct {
	TickerListener *broadcast.Listener

	W *os.File
}

func (s *FileConsumer) processTicker(ticker *model.Ticker) {
	s.W.Write([]byte(fmt.Sprintf(
		"%s source=%s base=%s quote=%s last_price=%s ts=%d\n",
		time.Now().Format(constants.TS_FORMAT), ticker.Source, ticker.Base, ticker.Quote, ticker.LastPrice, ticker.Timestamp.UTC().UnixMilli())))
}

// Listen for tickers in the ch channel and sends them to a io.Writer
func (s *FileConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	go func() {
		log.Info("IOWriter consumer listening now for tickers")
		for ticker := range s.TickerListener.Channel() {
			s.processTicker(ticker.(*model.Ticker))
		}

		s.W.Close()
	}()

}
func (s *FileConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
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
