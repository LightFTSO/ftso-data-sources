package flags

import "flag"

var (
	ConfigFile = flag.String("config", "./config.yaml", "Use this configuration file")
)
