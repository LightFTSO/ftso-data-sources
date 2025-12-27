package model

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/go-multierror"
	"roselabs.mx/ftso-data-sources/constants"
)

type Ticker struct {
	Base      string    `json:"b"`
	Quote     string    `json:"q"`
	Source    string    `json:"S"`
	Price     float64   `json:"l"`
	Timestamp time.Time `json:"ts"`
}

func (t *Ticker) Validate() error {
	var errorList *multierror.Error

	err := t.validateBase()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}
	err = t.validateQuote()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}
	err = t.validateSource()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}
	err = t.validateTimestamp()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}

	return errorList.ErrorOrNil()
}

func (t *Ticker) validateBase() error {
	if t.Base == "" {
		return fmt.Errorf("base \"%s\" is invalid", t.Base)
	}
	return nil
}

func (t *Ticker) validateQuote() error {
	validQuote := constants.IsValidQuote(t.Quote)
	if !validQuote {
		return fmt.Errorf("%s is not a valid quote asset", t.Quote)
	}
	return nil
}

func (t *Ticker) validateSource() error {
	if t.Source == "" {
		return fmt.Errorf("source \"%s\" is invalid", t.Source)
	}
	return nil
}
func (t *Ticker) validateTimestamp() error {
	if t.Timestamp.IsZero() {
		t.Timestamp = time.Now()
		return nil
	}
	return nil
}

func (t *Ticker) Symbol() string {
	return t.Base + "/" + t.Quote
}

func NewTicker(price float64,
	symbol Symbol,
	source string,
	timestamp time.Time) (*Ticker, error) {

	ticker := Ticker{
		Price:     price,
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Source:    source,
		Timestamp: timestamp.UTC(),
	}

	err := ticker.Validate()

	return &ticker, err
}

func NewTickerPriceString(priceString string,
	symbol Symbol,
	source string,
	timestamp time.Time) (*Ticker, error) {

	price, err := strconv.ParseFloat(priceString, 64)
	if err != nil {
		return nil, fmt.Errorf("lastPrice:\"%s\" is not a valid price: %w", priceString, err)
	}

	ticker := Ticker{
		Price:     price,
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Source:    source,
		Timestamp: timestamp.UTC(),
	}

	err = ticker.Validate()

	return &ticker, err
}
