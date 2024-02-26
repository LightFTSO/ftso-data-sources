package datasource

type FtsoDataSource interface {
	StartTrades() error
	SubscribeTrades([]string, []string) ([]string, error)

	//StartTickers(chan<- map[string]interface{}) error
	//SubscribeTickers([]string, []string) ([]string, error)
	Connect() error
	Close() error
}

type DataSourceList []FtsoDataSource
