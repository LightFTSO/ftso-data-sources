package model

import "time"

type Trade struct {
	Base      string    `json:"base"`
	Quote     string    `json:"quote"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Side      string    `json:"side"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"ts"`
}
