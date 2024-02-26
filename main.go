package main

import (
	"fmt"
	log "log/slog"
	"os"
	"sync"

	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/factory"
	"roselabs.mx/ftso-data-sources/model"
)

func main() {
	var w sync.WaitGroup

	fmt.Println("Hell yeah")

	tradeChan := make(chan model.Trade)

	stdoutConsumer, _ := consumer.NewIOWriterConsumer(os.Stdout, tradeChan)
	stdoutConsumer.StartTradeListener()

	/*outFile, err := os.OpenFile("out.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	fileConsumer, _ := consumer.NewIOWriterConsumer(outFile, tradeChan)
	fileConsumer.StartTradeListener()*/

	dataSourceList := []string{
		"binance",
		"noisy",
	}

	dataSources := make(datasource.DataSourceList, len(dataSourceList))

	for i, source := range dataSourceList {
		src, err := factory.BuilDataSource(source, &tradeChan, &w)
		if err != nil {
			log.Error("Error creating data source", "type", source, "error", err.Error())
		}
		dataSources[i] = src
		log.Info("Created new datasource", "kind", source)
	}

	for _, src := range dataSources {
		quotes := []string{"xrp"}
		src.SubscribeTrades(quotes, constants.USD_USDT_USDC_BUSD[:])
		src.StartTrades()
	}

	// wait for all datasources to exit
	w.Wait()
}
