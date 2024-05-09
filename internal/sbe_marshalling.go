package internal

import (
	"bytes"

	"github.com/shopspring/decimal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/sbe/sbe"
)

var sbeMarshaller = sbe.NewSbeGoMarshaller()

func MarshalSbe(ticker model.Ticker) ([]byte, error) {

	var base [6]byte
	copy(base[:], ticker.Base)
	var quote [6]byte
	copy(quote[:], ticker.Quote)

	price, _ := decimal.NewFromString(ticker.LastPrice)

	sbeTicker := sbe.Ticker{
		Timestamp: uint64(ticker.Timestamp.UnixMilli()),
		Symbol: sbe.Symbol{
			Base:  base,
			Quote: quote,
		},
		Price: sbe.Decimal{
			Mantissa: uint64(price.CoefficientInt64()),
			Exponent: int8(price.Exponent()),
		},
	}
	//sbe.TickerInit(&sbeTicker)
	var buf = new(bytes.Buffer)

	header := sbe.SbeGoMessageHeader{
		BlockLength: sbeTicker.SbeBlockLength(),
		TemplateId:  sbeTicker.SbeTemplateId(),
		SchemaId:    sbeTicker.SbeSchemaId(),
		Version:     sbeTicker.SbeSchemaVersion(),
	}
	header.Encode(sbeMarshaller, buf)
	if err := sbeTicker.Encode(sbeMarshaller, buf, true); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
