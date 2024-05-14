package consumer

import (
	log "log/slog"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"golang.org/x/net/websocket"
	"roselabs.mx/ftso-data-sources/internal"
	websocket_server "roselabs.mx/ftso-data-sources/internal/websocket_server"
	"roselabs.mx/ftso-data-sources/model"
)

type WebsocketConsumerOptions struct {
	Enabled             bool   `mapstructure:"enabled"`
	Host                string `mapstructure:"host"`
	Port                int    `mapstructure:"port"`
	TickersEndpoint     string `mapstructure:"ticker_endpoint"`
	UseSbeEncoding      bool   `mapstructure:"use_sbe_encoding"`
	IndividualFeedTable bool   `mapstructure:"individual_feed_table"`
}

type WebsocketServerConsumer struct {
	wsServer       websocket_server.WebsocketServer
	TickerListener *broadcast.Listener

	config WebsocketConsumerOptions
}

func (s *WebsocketServerConsumer) setup() error {
	if err := s.wsServer.Connect(); err != nil {
		panic(err)
	}
	log.Info("Websocket Consumer started")

	return nil
}

func (s *WebsocketServerConsumer) processTicker(ticker *model.Ticker) {
	payload, err := sonic.Marshal(ticker)
	if err != nil {
		log.Error("error encoding ticker", "consumer", "websocket", "error", err)
	}
	err = s.wsServer.BroadcastMessage(websocket.TextFrame, payload)
	if err != nil {
		log.Error("error broadcasting ticker", "consumer", "websocket", "error", err)
	}
}

func (s *WebsocketServerConsumer) processTickerSbe(ticker *model.Ticker, sbeMarshaller *internal.SbeMarshaller) {
	payload, err := sbeMarshaller.MarshalSbe(*ticker)
	if err != nil {
		log.Error("error encoding ticker", "consumer", "websocket", "error", err)
	}
	err = s.wsServer.BroadcastMessage(websocket.BinaryFrame, payload)
	if err != nil {
		log.Error("error broadcasting ticker", "consumer", "websocket", "error", err)
	}

}

func (s *WebsocketServerConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers and sends them to a Websocket connection
	s.TickerListener = tickerTopic.Listen()
	log.Debug("Websocker ticker listening for tickers now", "consumer", "websocket", "address", s.wsServer.Address)
	if s.config.UseSbeEncoding {
		go func() {
			sbeMarshaller := internal.NewSbeGoMarshaller()
			for ticker := range s.TickerListener.Channel() {
				s.processTickerSbe(ticker.(*model.Ticker), &sbeMarshaller)
			}
		}()
	} else {
		go func() {
			for ticker := range s.TickerListener.Channel() {
				s.processTicker(ticker.(*model.Ticker))
			}
		}()
	}

}

func (s *WebsocketServerConsumer) CloseTickerListener() {

}

func NewWebsocketConsumer(options WebsocketConsumerOptions) *WebsocketServerConsumer {
	server := websocket_server.NewWebsocketServer(options.Host, options.Port, options.TickersEndpoint)

	newConsumer := &WebsocketServerConsumer{
		wsServer: *server,
		config:   options}
	newConsumer.setup()

	return newConsumer
}
