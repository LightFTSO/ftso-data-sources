package model

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/go-multierror"
	"roselabs.mx/ftso-data-sources/constants"
)

type Ticker struct {
	LastPrice        string    `json:"l"`
	LastPriceFloat64 float64   `json:"-"`
	Symbol           string    `json:"s"`
	Base             string    `json:"b"`
	Quote            string    `json:"q"`
	Source           string    `json:"S"`
	Timestamp        time.Time `json:"ts"`
}

func (t *Ticker) Validate() error {
	var errorList *multierror.Error
	err := t.validateLastPrice()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}
	err = t.validateSymbol()
	if err != nil {
		errorList = multierror.Append(errorList, err)
	}
	err = t.validateBase()
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

func (t *Ticker) validateLastPrice() error {
	lastPrice, err := strconv.ParseFloat(t.LastPrice, 64)
	if err != nil {
		return fmt.Errorf("lastPrice:\"%s\" is not a valid price: %w", t.LastPrice, err)
	}
	t.LastPriceFloat64 = lastPrice

	return nil
}

func (t *Ticker) validateSymbol() error {
	symbol := t.Base + "/" + t.Quote
	if t.Symbol != symbol {
		return fmt.Errorf("ticker symbol %s doesn't match base and quote assets %s/%s", t.Symbol, t.Base, t.Quote)
	}
	return nil
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

func NewTicker(lastPrice string,
	symbol Symbol,
	source string,
	timestamp time.Time) (*Ticker, error) {
	ticker := Ticker{
		LastPrice: lastPrice,
		Base:      symbol.Base,
		Quote:     symbol.Quote,
		Symbol:    symbol.Base + "/" + symbol.Quote,
		Source:    source,
		Timestamp: timestamp,
	}

	err := ticker.Validate()

	return &ticker, err
}
