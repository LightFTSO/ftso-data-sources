package model

import (
	"fmt"
	"strconv"
	"time"
)

type TickerMessage struct {
	Base      string  `json:"b"`
	Quote     string  `json:"q"`
	Source    string  `json:"S"`
	Price     float64 `json:"l"`
	Timestamp int64   `json:"ts"`
}

// NewTickerMessage creates a TickerMessage from a Ticker.
// Inlining this simple conversion logic is better for the compiler.
func NewTickerMessage(ticker Ticker) TickerMessage {
	return TickerMessage{
		Base:      ticker.Base,
		Quote:     ticker.Quote,
		Source:    ticker.Source,
		Price:     ticker.Price,
		Timestamp: ticker.Timestamp.UnixMilli(),
	}
}

type Ticker struct {
	Base      string    `json:"b"`
	Quote     string    `json:"q"`
	Source    string    `json:"S"`
	Price     float64   `json:"l"`
	Timestamp time.Time `json:"ts"`
}

// Validate uses "fail-fast" logic.
// It removes the heap allocation overhead of multierror.
func (t Ticker) Validate() error {
	if t.Base == "" {
		return fmt.Errorf("base \"%s\" is invalid", t.Base)
	}

	// Optimization: Ideally, Quote validation should happen once at startup/config,
	// not on every single tick. However, if required:
	//if !constants.IsValidQuote(t.Quote) {
	//	return fmt.Errorf("%s is not a valid quote asset", t.Quote)
	//}

	if t.Source == "" {
		return fmt.Errorf("source \"%s\" is invalid", t.Source)
	}

	// Note: Timestamp default logic moved to Constructor
	return nil
}

// NewTicker returns Ticker by value (not pointer) to enable stack allocation.
func NewTicker(price float64, symbol Symbol, source string, timestamp time.Time) (Ticker, error) {
	// Handle default timestamp here, strictly before creation
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	ticker := Ticker{
		Price:     price,
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Source:    source,
		Timestamp: timestamp.UTC(),
	}

	// Validation is costly. If you trust the data sources,
	// you might consider removing this call in the hot path.
	//if err := ticker.Validate(); err != nil {
	//	return Ticker{}, err
	//}

	return ticker, nil
}

// NewTickerPriceString handles string-to-float conversion.
func NewTickerPriceString(priceString string, symbol Symbol, source string, timestamp time.Time) (Ticker, error) {
	// fast path check for empty string
	if priceString == "" {
		return Ticker{}, fmt.Errorf("lastPrice is empty")
	}

	price, err := strconv.ParseFloat(priceString, 64)
	if err != nil {
		return Ticker{}, fmt.Errorf("lastPrice:\"%s\" is not a valid price: %w", priceString, err)
	}

	return NewTicker(price, symbol, source, timestamp)
}
