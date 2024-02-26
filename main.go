package main

import (
	"fmt"
	log "log/slog"
	"os"
	"sync"

	"roselabs.mx/ftso-data-sources/constants"
	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/factory"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

func main() {
	quotes := constants.BASES_CRYPTO[:] //[]string{"xrp"}
	symbols, err := symbols.CreateSymbolList(quotes, constants.USD_USDT_USDC_BUSD[:])
	if err != nil {
		panic(err)
	}

	log.Info("Symbol list", "symbols", symbols)

	var w sync.WaitGroup

	fmt.Println("Hell yeahhh")

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
		//"noisy",
	}

	//dataSources := make(datasource.DataSourceList, len(dataSourceList))

	for _, source := range dataSourceList {
		w.Add(1)
		go func(source string) {
			src, err := factory.BuilDataSource(source, symbols, &tradeChan, &w)
			if err != nil {
				log.Error("Error creating data source", "datasource", source, "error", err.Error())
				w.Done()
				return
			}
			err = src.Connect()
			if err != nil {
				log.Error("Error connecting", "datasource", src.GetName())
				w.Done()
				return
			}

			err = src.SubscribeTrades()
			if err != nil {
				log.Error("Error subscribing to trades", "datasource", src.GetName())
				w.Done()
				return
			}

			w.Done()
		}(source)
	}

	// wait for all datasources to exit
	w.Wait()
}
