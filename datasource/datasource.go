package datasource

type FtsoDataSource interface {
	SubscribeTrades() error
	//StartTickers(chan<- map[string]interface{}) error
	//SubscribeTickers([]string, []string) ([]string, error)
	Connect() error
	Reconnect() error
	Close() error
	GetName() string
}

type DataSourceList []FtsoDataSource
