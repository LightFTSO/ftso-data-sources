package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "log/slog"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/sbe/sbe"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConsumer struct {
	TradeListener  *broadcast.Listener
	TickerListener *broadcast.Listener

	numThreads int

	useSbeEncoding bool

	mqttClient mqtt.Client
	qosLevel   int
}

type MqttConsumerOptions struct {
	Enabled        bool
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

	token := s.mqttClient.Publish("trades/", byte(0), false, []byte("datatest"))
	token.Wait()
	//s.mqttClient.Disconnect(250)
	//fmt.Println("Sample Publisher Disconnected")
	return nil

}

func (s *MqttConsumer) processTrade(trade *model.Trade, sbeGoMarshaller *sbe.SbeGoMarshaller) {
	var payload []byte
	if s.useSbeEncoding {
		var base [6]byte
		copy(base[:], trade.Base)
		var quote [6]byte
		copy(quote[:], trade.Quote)
		sbeTrade := sbe.Trade{
			Timestamp: uint64(trade.Timestamp.UnixMilli()),
			Symbol: sbe.Symbol{
				Base:  base,
				Quote: quote,
			},
			Price: sbe.Decimal{
				Mantissa: 31416,
				Exponent: 2,
			},
			Size: sbe.Decimal{
				Mantissa: 1618,
				Exponent: 2,
			},
			Side:   []sbe.TradeSide{},
			Source: []uint8(trade.Source),
		}
		fmt.Println(sbeTrade, trade.Source)
		sbe.TradeInit(&sbeTrade)
		var buf = new(bytes.Buffer)

		header := sbe.SbeGoMessageHeader{
			BlockLength: sbeTrade.SbeBlockLength(),
			TemplateId:  sbeTrade.SbeTemplateId(),
			SchemaId:    sbeTrade.SbeSchemaId(),
			Version:     sbeTrade.SbeSchemaVersion(),
		}
		header.Encode(sbeGoMarshaller, buf)
		if err := sbeTrade.Encode(sbeGoMarshaller, buf, true); err != nil {
			/// handle errors
			log.Error("error encoding trade", "error", err)
		}
		token := s.mqttClient.Publish("trades/", byte(s.qosLevel), false, buf.Bytes())
		token.Wait()

	} else {
		payload, _ = json.Marshal(trade)
		token := s.mqttClient.Publish("trades/", byte(s.qosLevel), false, payload)
		token.Wait()
	}

}

func (s *MqttConsumer) StartTradeListener(tradeTopic *broadcast.Broadcaster) {
	// Listen for trades in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Mosquitto trade listener configured with %d consumer goroutines", s.numThreads), "consumer", "mosquitto", "num_threads", s.numThreads)
	s.TradeListener = tradeTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			m := sbe.NewSbeGoMarshaller()
			log.Debug(fmt.Sprintf("Mosquitto trade consumer %d listening for trades now", consumerId), "consumer", "mosquitto", "consumer_num", consumerId)
			for trade := range s.TradeListener.Channel() {
				s.processTrade(trade.(*model.Trade), m)
			}
		}(consumerId)
	}

}
func (s *MqttConsumer) CloseTradeListener() {
	s.TradeListener.Discard()
}

func (s *MqttConsumer) processTicker(ticker *model.Ticker) {
	payload := fmt.Sprintf(
		"%s source=%s symbol=%s last_price=%f ts=%d\n",
		time.Now().Format(constants.TS_FORMAT), ticker.Source, ticker.Symbol, ticker.LastPrice, ticker.Timestamp.UTC().UnixMilli())

	token := s.mqttClient.Publish("tickers", byte(s.qosLevel), false, payload)
	token.Wait()
}

func (s *MqttConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for trades in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Mosquitto trade listener configured with %d consumer goroutines", s.numThreads), "consumer", "mosquitto", "num_threads", s.numThreads)
	s.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Mosquitto ticker consumer %d listening for tickers now", consumerId), "consumer", "mosquitto", "consumer_num", consumerId)
			for ticker := range s.TradeListener.Channel() {
				s.processTicker(ticker.(*model.Ticker))
			}
		}(consumerId)
	}

}
func (s *MqttConsumer) CloseTickerListener() {
	s.TickerListener.Discard()
}

func NewMqttConsumer(options MqttConsumerOptions) *MqttConsumer {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:1883")
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
