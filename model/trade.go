package model

import "time"

type Trade struct {
	Base      string    `json:"-"`
	Quote     string    `json:"-"`
	Symbol    string    `json:"s"`
	Price     float64   `json:"p"`
	Size      float64   `json:"Q"`
	Side      string    `json:"b"`
	Source    string    `json:"S"`
	Timestamp time.Time `json:"ts"`
}
