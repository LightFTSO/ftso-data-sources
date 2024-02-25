package factory

import "fmt"

type ExchangeWSClient interface {
	Connect() error
	Listen([]string, func(trade *model.Trade)) error
	Close()
}

func BuildWSClient(exchange, APIKey, APISecret string) (ExchangeWSClient, error) {
	if exchange == "FTX" {
		return exchanges.NewFTXWSClient(APIKey, APISecret), nil
	}
	return nil, fmt.Errorf("exchange: %s doesn't exists", exchange)
}
