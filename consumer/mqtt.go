package consumer

import (
	"fmt"
	log "log/slog"
	"time"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConsumer struct {
	TickerListener *broadcast.Listener

	numThreads int

	useSbeEncoding bool

	mqttClient           mqtt.Client
	qosLevel             int
	useExchangeTimestamp bool
}

type MqttConsumerOptions struct {
	Enabled        bool
	Url            string             `mapstructure:"url"`
	ClientOptions  mqtt.ClientOptions `mapstructure:"client_options"`
	NumThreads     int                `mapstructure:"num_threads"`
	UseSbeEncoding bool               `mapstructure:"use_sbe_encoding"`
	QOSLevel       int                `mapstructure:"qos_level"`
}

func (s *MqttConsumer) setup() error {

	if token := s.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Info("MQTT Consumer started")

	return nil

}

func (s *MqttConsumer) processTicker(ticker *model.Ticker, sbeMarshaller *internal.SbeMarshaller) {
	if !s.useExchangeTimestamp {
		ticker.Timestamp = time.Now().UTC()
	}

	channel := fmt.Sprintf("tickers/%s/%s/%s", ticker.Base, ticker.Quote, ticker.Source)

	if s.useSbeEncoding {
		payload, err := sbeMarshaller.MarshalSbe(*ticker)
		if err != nil {
			log.Error("error encoding ticker", "consumer", "mqtt", "error", err)
		}
		token := s.mqttClient.Publish(channel, byte(s.qosLevel), false, payload)
		token.Wait()
	} else {
		payload, err := sonic.Marshal(ticker)
		if err != nil {
			log.Error("error encoding ticker", "consumer", "mqtt", "error", err)
		}
		token := s.mqttClient.Publish(channel, byte(s.qosLevel), false, payload)
		token.Wait()
	}
}

func (s *MqttConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers in the ch channel and sends them to an MQTT broker
	log.Debug(fmt.Sprintf("MQTT ticker listener configured with %d consumer goroutines", s.numThreads), "consumer", "mqtt", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			sbeMarshaller := internal.NewSbeGoMarshaller()
			log.Debug(fmt.Sprintf("MQTT ticker consumer %d listening for tickers now", consumerId), "consumer", "mqtt", "consumer_num", consumerId)
			for ticker := range s.TickerListener.Channel() {
				s.processTicker(ticker.(*model.Ticker), &sbeMarshaller)
			}
		}(consumerId)
	}

}

func (s *MqttConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewMqttConsumer(options MqttConsumerOptions, useExchangeTimestamp bool) *MqttConsumer {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(options.Url)
	opts.SetCleanSession(false)
	//opts.SetDefaultPublishHandler()
	opts.SetAutoReconnect(true)
	opts.SetUsername("")
	opts.SetPassword("")
	opts.SetClientID("ftso-data-sources")

	newConsumer := &MqttConsumer{
		mqttClient:           mqtt.NewClient(opts),
		numThreads:           options.NumThreads,
		useSbeEncoding:       options.UseSbeEncoding,
		qosLevel:             options.QOSLevel,
		useExchangeTimestamp: useExchangeTimestamp,
	}
	newConsumer.setup()

	return newConsumer
}
