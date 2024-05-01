package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "log/slog"

	"github.com/shopspring/decimal"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/sbe/sbe"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConsumer struct {
	TickerListener *broadcast.Listener

	numThreads int

	useSbeEncoding bool

	mqttClient mqtt.Client
	qosLevel   int
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
	fmt.Println("Sample Publisher Started")

	token := s.mqttClient.Publish("info/", byte(0), false, []byte("datatest"))
	token.Wait()
	//s.mqttClient.Disconnect(250)
	//fmt.Println("Sample Publisher Disconnected")
	return nil

}

func (s *MqttConsumer) processTicker(ticker *model.Ticker, sbeGoMarshaller *sbe.SbeGoMarshaller) {
	var payload []byte
	if s.useSbeEncoding {
		var base [6]byte
		copy(base[:], ticker.Base)
		var quote [6]byte
		copy(quote[:], ticker.Quote)

		price, _ := decimal.NewFromString(ticker.LastPrice)
		//fmt.Println(price.Exponent())
		sbeTicker := sbe.Ticker{
			Timestamp: uint64(ticker.Timestamp.UnixMilli()),
			Symbol: sbe.Symbol{
				Base:  base,
				Quote: quote,
			},
			Last_price: sbe.Decimal{
				Mantissa: uint64(price.CoefficientInt64()),
				Exponent: int8(price.Exponent()),
			},
			Source: []uint8(ticker.Source),
		}
		sbe.TickerInit(&sbeTicker)
		var buf = new(bytes.Buffer)

		header := sbe.SbeGoMessageHeader{
			BlockLength: sbeTicker.SbeBlockLength(),
			TemplateId:  sbeTicker.SbeTemplateId(),
			SchemaId:    sbeTicker.SbeSchemaId(),
			Version:     sbeTicker.SbeSchemaVersion(),
		}
		header.Encode(sbeGoMarshaller, buf)
		if err := sbeTicker.Encode(sbeGoMarshaller, buf, true); err != nil {
			/// handle errors
			log.Error("error encoding trade", "error", err)
		}
		token := s.mqttClient.Publish("tickers/", byte(s.qosLevel), false, buf.Bytes())
		//fmt.Println(buf.Bytes())
		token.Wait()

	} else {
		payload, _ = json.Marshal(ticker)
		token := s.mqttClient.Publish("tickers/", byte(s.qosLevel), false, payload)
		token.Wait()
	}
}

func (s *MqttConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Mosquitto ticker listener configured with %d consumer goroutines", s.numThreads), "consumer", "mosquitto", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			m := sbe.NewSbeGoMarshaller()
			log.Debug(fmt.Sprintf("Mosquitto ticker consumer %d listening for tickers now", consumerId), "consumer", "mosquitto", "consumer_num", consumerId)
			for ticker := range s.TickerListener.Channel() {
				s.processTicker(ticker.(*model.Ticker), m)
			}
		}(consumerId)
	}

}

func (s *MqttConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewMqttConsumer(options MqttConsumerOptions) *MqttConsumer {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(options.Url)
	opts.SetCleanSession(false)
	//opts.SetDefaultPublishHandler()
	opts.SetAutoReconnect(true)
	opts.SetUsername("")
	opts.SetPassword("")
	opts.SetClientID("ftso-data-sources")

	newConsumer := &MqttConsumer{
		mqttClient:     mqtt.NewClient(opts),
		numThreads:     options.NumThreads,
		useSbeEncoding: options.UseSbeEncoding,
		qosLevel:       options.QOSLevel,
	}
	newConsumer.setup()

	return newConsumer
}
