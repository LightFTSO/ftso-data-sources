package model

import "time"

type Ticker struct {
	LastPrice string    `json:"l"`
	Symbol    string    `json:"s"`
	Base      string    `json:"b"`
	Quote     string    `json:"q"`
	Source    string    `json:"S"`
	Timestamp time.Time `json:"ts"`
}
