package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"roselabs.mx/ftso-data-sources/consumer"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/model"
)

func main() {
	var w sync.WaitGroup

	fmt.Println("Hell yeah")

	tradeChan := make(chan model.Trade)

	/*outFile, err := os.OpenFile("out.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	fileConsumer, _ := consumer.NewIOWriterConsumer(outFile, tradeChan)
	fileConsumer.StartTradeListener()*/

	stdoutConsumer, _ := consumer.NewIOWriterConsumer(os.Stdout, tradeChan)
	stdoutConsumer.StartTradeListener()

	noisy1 := datasource.NewNoisySource("noisy1", 4*time.Millisecond, &tradeChan, &w)
	noisy1.StartTrades()

	noisy2 := datasource.NewNoisySource("noisy2", 5*time.Millisecond, &tradeChan, &w)
	noisy2.StartTrades()

	// wait for all datasources to exit
	w.Wait()
}
