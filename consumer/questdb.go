package consumer

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	questdb "github.com/questdb/go-questdb-client/v3"
	"github.com/textileio/go-threads/broadcast"
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
	questdbSender        *questdb.LineSender
	TickerListener       *broadcast.Listener
	individualFeedTable  bool
	flushInterval        time.Duration
	txContext            *context.Context
	useExchangeTimestamp bool
}

func (q *QuestDbConsumer) processTicker(ticker *model.Ticker) {
	//fmt.Printf("tickers,%s,%s,%s,%s,%v,%v\n", ticker.Base, ticker.Quote, ticker.Source, ticker.LastPrice, constants.IsStablecoin(ticker.Base), ticker.Timestamp)

	if !q.useExchangeTimestamp {
		ticker.Timestamp = time.Now().UTC()
	}

	var err error
	if q.individualFeedTable {
		tableName := fmt.Sprintf("%s_tickers", ticker.Base)
		err = (*q.questdbSender).Table(tableName).
			Symbol("exchange", ticker.Source).
			Symbol("base", ticker.Base).
			Symbol("quote", ticker.Quote).
			Float64Column("price", ticker.LastPriceFloat64).
			At(*q.txContext, ticker.Timestamp)
	} else {
		err = (*q.questdbSender).Table("tickers").
			Symbol("base", ticker.Base).
			Symbol("quote", ticker.Quote).
			Symbol("exchange", ticker.Source).
			Float64Column("price", ticker.LastPriceFloat64).
			At(*q.txContext, ticker.Timestamp)
	}
	if err != nil {
		log.Error("Error processing ticker for ILP", "consumer", "questdb", "error", err)
	}
}

func (q *QuestDbConsumer) StartTickerListener(tickerTopic *broadcast.Broadcaster) {
	// Listen for tickers in the ch channel and sends them to a io.Writer
	log.Debug("QuestDB ILP ticker listener configured", "consumer", "questdb")
	q.TickerListener = tickerTopic.Listen()
	go func() {
		log.Debug("QuestDB ILP ticker consumer listening for tickers now", "consumer", "questdb")
		for ticker := range q.TickerListener.Channel() {
			q.processTicker(ticker.(*model.Ticker))
		}
	}()

}
func (q *QuestDbConsumer) CloseTickerListener() {
	q.TickerListener.Discard()
}

func NewQuestDbConsumer(options QuestDbConsumerOptions, useExchangeTimestamp bool) *QuestDbConsumer {
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
		options.FlushInterval = minFlushInterval
		log.Warn(fmt.Sprintf("The flush interval %v is set to less than the current minimum %v, using %v",
			options.FlushInterval, minFlushInterval, minFlushInterval))
	}
	if schema == "http" || schema == "https" {
		configString = fmt.Sprintf("%sauto_flush_interval=%d;", configString, options.FlushInterval)
	} else {
		log.Warn(fmt.Sprintf("Flush interval %v only applies to senders using the http(s) schema",
			options.FlushInterval))
	}

	ctx := context.TODO()
	log.Info(fmt.Sprintf("Connecting to QuestDB at %s", options.ClientOptions.Address), "consumer", "questdb")
	log.Debug(configString, "consumer", "questdb")
	sender, err := questdb.LineSenderFromConf(ctx, configString)
	if err != nil {
		panic(err)
	}

	newConsumer := &QuestDbConsumer{
		questdbSender:        &sender,
		individualFeedTable:  options.IndividualFeedTable,
		flushInterval:        options.FlushInterval,
		txContext:            &ctx,
		useExchangeTimestamp: useExchangeTimestamp,
	}
	return newConsumer
}
