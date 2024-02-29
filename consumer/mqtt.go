package consumer

import (
	"encoding/json"
	"fmt"
	log "log/slog"
	"time"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttConsumer struct {
	TradeListener  *broadcast.Listener
	TickerListener *broadcast.Listener

	numThreads int

	mqttClient mqtt.Client
}

type MqttConsumerOptions struct {
	Enabled       bool
	ClientOptions mqtt.ClientOptions `mapstructure:"client_options"`
	NumThreads    int                `mapstructure:"num_threads"`
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

func (s *MqttConsumer) processTrade(trade *model.Trade) {
	/*payload := fmt.Sprintf(
	"%s source=%s symbol=%s price=%f size=%f side=%s ts=%d\n",
	time.Now().Format(constants.TS_FORMAT), trade.Source, trade.Symbol, trade.Price, trade.Size, trade.Side, trade.Timestamp.UTC().UnixMilli())*/
	payload, _ := json.Marshal(trade)
	token := s.mqttClient.Publish("trades/", byte(0), false, payload)
	token.Wait()

	/*if err != nil {
		log.Error("Error executing ts.ADD", "consumer", "mosquitto", "error", err)
	}*/
}

func (s *MqttConsumer) StartTradeListener(tradeTopic *broadcast.Broadcaster) {
	// Listen for trades in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("Mosquitto trade listener configured with %d consumer goroutines", s.numThreads), "consumer", "mosquitto", "num_threads", s.numThreads)
	s.TradeListener = tradeTopic.Listen()
	for consumerId := 1; consumerId <= s.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("Mosquitto trade consumer %d listening for trades now", consumerId), "consumer", "mosquitto", "consumer_num", consumerId)
			for trade := range s.TradeListener.Channel() {
				s.processTrade(trade.(*model.Trade))
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

	token := s.mqttClient.Publish("tickers", byte(0), false, payload)
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
		mqttClient: mqtt.NewClient(opts),
		numThreads: options.NumThreads,
	}
	newConsumer.setup()

	return newConsumer
}
