package consumer

import (
	"fmt"
	log "log/slog"

	"github.com/textileio/go-threads/broadcast"
	"golang.org/x/exp/rand"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/tickertopic"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConsumer struct {
	TickerListener *broadcast.Listener

	numThreads int

	mqttClient mqtt.Client
	qosLevel   int
}

type MqttConsumerOptions struct {
	Enabled       bool
	Url           string             `mapstructure:"url"`
	ClientOptions mqtt.ClientOptions `mapstructure:"client_options"`
	QOSLevel      int                `mapstructure:"qos_level"`
}

func (s *MqttConsumer) setup() error {

	if token := s.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Info("MQTT Consumer started")

	return nil

}

func (s *MqttConsumer) processTicker(ticker *model.Ticker) {
	channel := fmt.Sprintf("tickers/%s/%s/%s", ticker.Base, ticker.Quote, ticker.Source)

	payload := fmt.Sprintf("%s,%d", ticker.Price, ticker.Timestamp.UnixMilli())
	token := s.mqttClient.Publish(channel, byte(s.qosLevel), false, payload)
	token.Wait()
}

func (s *MqttConsumer) StartTickerListener(tickerTopic *tickertopic.TickerTopic) {
	// Listen for tickers in the ch channel and sends them to an MQTT broker
	log.Debug(fmt.Sprintf("MQTT ticker listener configured with %d consumer goroutines", s.numThreads), "consumer", "mqtt")
	s.TickerListener = tickerTopic.Broadcaster.Listen()
	go func() {
		log.Debug("MQTT ticker consumer listening for tickers now", "consumer", "mqtt")
		for ticker := range s.TickerListener.Channel() {
			s.processTicker(ticker.(*model.Ticker))
		}
	}()

}

func (s *MqttConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewMqttConsumer(options MqttConsumerOptions) *MqttConsumer {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(options.Url)
	opts.SetCleanSession(true)
	//opts.SetDefaultPublishHandler()
	opts.SetAutoReconnect(true)
	opts.SetUsername("")
	opts.SetPassword("")
	opts.SetClientID((func(n int) string {
		const letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
		}
		return string(b)
	})(12)) // create a random ClientID

	newConsumer := &MqttConsumer{
		mqttClient: mqtt.NewClient(opts),
		qosLevel:   options.QOSLevel,
	}
	newConsumer.setup()

	return newConsumer
}
