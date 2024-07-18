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
	TickerListener *broadcast.Listener

	W                    *os.File
	useExchangeTimestamp bool
}

func (s *FileConsumer) processTicker(ticker *model.Ticker) {
	if !s.useExchangeTimestamp {
		ticker.Timestamp = time.Now().UTC()
	}

	s.W.Write([]byte(fmt.Sprintf(
		"%s source=%s symbol=%s last_price=%s ts=%d\n",
		time.Now().Format(constants.TS_FORMAT), ticker.Source, ticker.Symbol, ticker.LastPrice, ticker.Timestamp.UTC().UnixMilli())))
}

// Listen for tickers in the ch channel and sends them to a io.Writer
func (s *FileConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	s.TickerListener = tickerTopic.Listen()
	go func() {
		log.Info("IOWriter consumer listening now for tickers")
		for ticker := range s.TickerListener.Channel() {
			s.processTicker(ticker.(*model.Ticker))
		}

		s.W.Close()
	}()

}
func (s *FileConsumer) CloseTickerListener() {

}

func NewFileConsumer(filename string, useExchangeTimestamp bool) *FileConsumer {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	newConsumer := &FileConsumer{
		W:                    file,
		useExchangeTimestamp: useExchangeTimestamp,
	}
	return newConsumer
}
