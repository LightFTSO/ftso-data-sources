package model

import "time"

type Trade struct {
	Base      string    `json:"b"`
	Quote     string    `json:"q"`
	Symbol    string    `json:"s"`
	Price     string    `json:"p"`
	Size      string    `json:"Q"`
	Side      string    `json:"o"`
	Source    string    `json:"S"`
	Timestamp time.Time `json:"ts"`
}
