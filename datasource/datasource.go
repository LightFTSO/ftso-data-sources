package datasource

type FtsoDataSource interface {
	Start(chan<- map[string]interface{}) error
	Subscribe([]string, []string) ([]string, error)
	Close() error
}
