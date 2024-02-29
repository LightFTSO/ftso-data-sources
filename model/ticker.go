package model

import "time"

type Ticker struct {
	LastPrice float64   `json:"l"`
	Symbol    string    `json:"s"`
	Base      string    `json:"-"`
	Quote     string    `json:"-"`
	Source    string    `json:"S"`
	Timestamp time.Time `json:"ts"`
}
