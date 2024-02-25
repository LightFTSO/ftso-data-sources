package model

import "time"

type Ticker struct {
	LastPrice float64
	Exchange  string
	Base      string
	Quote     string
	Timestamp time.Time
}
