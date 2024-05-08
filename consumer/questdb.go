package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"strconv"
	"strings"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/model"
)

type QuestDbConsumerOptions struct {
	Enabled             bool          `mapstructure:"enabled"`
	FlushInterval       time.Duration `mapstructure:"flush_interval"`
	IndividualFeedTable bool          `mapstructure:"individual_feed_table"`
	NumThreads          int           `mapstructure:"num_threads"`
	ClientOptions       struct {
		Schema    string `mapstructure:"schema"`
		Address   string `mapstructure:"address"`
		Username  string `mapstructure:"username"`
		Password  string `mapstructure:"password"`
		TlsVerify bool   `mapstructure:"tls_verify"`
	} `mapstructure:"client_options"`
}

type QuestDbConsumer struct {
	questdbSender       *questdb.LineSender
	TickerListener      *broadcast.Listener
	individualFeedTable bool
	numThreads          int
	flushInterval       time.Duration
	flushIntervalTicker *time.Ticker
	txContext           *context.Context
}

func (q *QuestDbConsumer) setup() error {

	go func() {
		log.Info("Setting up QuestDB ILP sender", "consumer", "questdb")
		q.flushIntervalTicker = time.NewTicker(q.flushInterval)
		defer q.flushIntervalTicker.Stop()

		for range q.flushIntervalTicker.C {
			q.flushILPBuffer()
		}
	}()

	return nil
}

func (q *QuestDbConsumer) flushILPBuffer() error {
	log.Debug("Flushing ILP buffer", "consumer", "questdb")

	if err := (*q.questdbSender).Flush(*q.txContext); err != nil {
		log.Error("Error ILP buffer", "consumer", "questdb", "error", err)
	}
	return nil
}

func (q *QuestDbConsumer) processTicker(ticker *model.Ticker) {
	lastPrice, err := strconv.ParseFloat(ticker.LastPrice, 64)
	if err != nil {
		log.Error("Error formatting price as float64 for ILP", "consumer", "questdb", "error", err)
		return
	}
	if q.individualFeedTable {
		tableName := fmt.Sprintf("%s_tickers", ticker.Base)
		err := (*q.questdbSender).Table(tableName).
			Symbol("exchange", ticker.Source).
			Symbol("base", ticker.Base).
			Symbol("quote", ticker.Quote).
			Float64Column("price", lastPrice).
			BoolColumn("stablecoin", constants.IsStablecoin(ticker.Base)).
			At(*q.txContext, ticker.Timestamp)
		if err != nil {
			log.Error("Error processing ticker for ILP", "consumer", "questdb", "error", err)
		}
	} else {
		//fmt.Printf("tickers,%s,%s,%s,%s,%v,%v\n", ticker.Base, ticker.Quote, ticker.Source, ticker.LastPrice, constants.IsStablecoin(ticker.Base), ticker.Timestamp)
		err := (*q.questdbSender).Table("tickers").
			Symbol("base", ticker.Base).
			Symbol("quote", ticker.Quote).
			Symbol("exchange", ticker.Source).
			Float64Column("price", lastPrice).
			BoolColumn("stablecoin", constants.IsStablecoin(ticker.Base)).
			At(*q.txContext, ticker.Timestamp)
		if err != nil {
			log.Error("Error processing ticker for ILP", "consumer", "questdb", "error", err)
		}
	}
	//fmt.Println(ticker)

}

func (q *QuestDbConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers in the ch channel and sends them to a io.Writer
	log.Debug(fmt.Sprintf("QuestDB ILP ticker listener configured with %d consumer goroutines", q.numThreads), "consumer", "questdb", "num_threads", q.numThreads)
	q.TickerListener = tickerTopic.Listen()
	for consumerId := 1; consumerId <= q.numThreads; consumerId++ {
		go func(consumerId int) {
			log.Debug(fmt.Sprintf("QuestDB ILP ticker consumer %d listening for tickers now", consumerId), "consumer", "questdb", "consumer_num", consumerId)
			for ticker := range q.TickerListener.Channel() {
				q.processTicker(ticker.(*model.Ticker))
			}
		}(consumerId)
	}

}
func (q *QuestDbConsumer) CloseTickerListener() {
	q.TickerListener.Discard()
}

func NewQuestDbConsumer(options QuestDbConsumerOptions) *QuestDbConsumer {
	schema := options.ClientOptions.Schema
	if schema != "http" && schema != "https" && schema != "tcp" && schema != "tcps" {
		panic("QuestDB schema must be one of http,https,tcp,tcps")
	}

	configString := fmt.Sprintf("%s::addr=%s;", schema, options.ClientOptions.Address)
	if len(options.ClientOptions.Username) > 0 {
		configString = fmt.Sprintf("%susername=%s;", configString, options.ClientOptions.Username)
	}
	if len(options.ClientOptions.Password) > 0 {
		configString = fmt.Sprintf("%spassword=%s;", configString, options.ClientOptions.Username)
	}
	if strings.Contains(options.ClientOptions.Schema, "s") {
		if options.ClientOptions.TlsVerify {
			configString = fmt.Sprintf("%stls_verify=on;", configString)
		} else {
			configString = fmt.Sprintf("%stls_verify=unsafe_off;", configString)
		}
	}

	//configString = fmt.Sprintf("%sinit_buf_size=%s;",configString,)

	minFlushInterval := time.Duration(1) * time.Second
	if options.FlushInterval < minFlushInterval {
		log.Warn(fmt.Sprintf("The flush interval %v is set to less than the current minimum %v, using %v",
			options.FlushInterval, minFlushInterval, minFlushInterval))
	}

	ctx := context.TODO()
	log.Info(fmt.Sprintf("Connecting to QuestDB at %s", options.ClientOptions.Address), "consumer", "questdb")
	log.Debug(configString, "consumer", "questdb")
	sender, err := questdb.LineSenderFromConf(ctx, configString)
	if err != nil {
		panic(err)
	}

	newConsumer := &QuestDbConsumer{
		questdbSender:       &sender,
		individualFeedTable: options.IndividualFeedTable,
		numThreads:          options.NumThreads,
		flushInterval:       options.FlushInterval,
		txContext:           &ctx,
	}
	newConsumer.setup()

	return newConsumer
}
