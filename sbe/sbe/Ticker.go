// Generated SBE (Simple Binary Encoding) message codec

package sbe

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type Ticker struct {
	Timestamp uint64
	Price     Decimal
	Symbol    Symbol
}

func (t *Ticker) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := t.RangeCheck(t.SbeSchemaVersion(), t.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteUint64(_w, t.Timestamp); err != nil {
		return err
	}
	if err := t.Price.Encode(_m, _w); err != nil {
		return err
	}
	if err := t.Symbol.Encode(_m, _w); err != nil {
		return err
	}
	return nil
}

func (t *Ticker) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !t.TimestampInActingVersion(actingVersion) {
		t.Timestamp = t.TimestampNullValue()
	} else {
		if err := _m.ReadUint64(_r, &t.Timestamp); err != nil {
			return err
		}
	}
	if t.PriceInActingVersion(actingVersion) {
		if err := t.Price.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if t.SymbolInActingVersion(actingVersion) {
		if err := t.Symbol.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if actingVersion > t.SbeSchemaVersion() && blockLength > t.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-t.SbeBlockLength()))
	}
	if doRangeCheck {
		if err := t.RangeCheck(actingVersion, t.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (t *Ticker) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if t.TimestampInActingVersion(actingVersion) {
		if t.Timestamp < t.TimestampMinValue() || t.Timestamp > t.TimestampMaxValue() {
			return fmt.Errorf("Range check failed on t.Timestamp (%v < %v > %v)", t.TimestampMinValue(), t.Timestamp, t.TimestampMaxValue())
		}
	}
	return nil
}

func TickerInit(t *Ticker) {
	return
}

func (*Ticker) SbeBlockLength() (blockLength uint16) {
	return 29
}

func (*Ticker) SbeTemplateId() (templateId uint16) {
	return 2
}

func (*Ticker) SbeSchemaId() (schemaId uint16) {
	return 1
}

func (*Ticker) SbeSchemaVersion() (schemaVersion uint16) {
	return 0
}

func (*Ticker) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*Ticker) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*Ticker) TimestampId() uint16 {
	return 1
}

func (*Ticker) TimestampSinceVersion() uint16 {
	return 0
}

func (t *Ticker) TimestampInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.TimestampSinceVersion()
}

func (*Ticker) TimestampDeprecated() uint16 {
	return 0
}

func (*Ticker) TimestampMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*Ticker) TimestampMinValue() uint64 {
	return 0
}

func (*Ticker) TimestampMaxValue() uint64 {
	return math.MaxUint64 - 1
}

func (*Ticker) TimestampNullValue() uint64 {
	return math.MaxUint64
}

func (*Ticker) PriceId() uint16 {
	return 2
}

func (*Ticker) PriceSinceVersion() uint16 {
	return 0
}

func (t *Ticker) PriceInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.PriceSinceVersion()
}

func (*Ticker) PriceDeprecated() uint16 {
	return 0
}

func (*Ticker) PriceMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return "Price"
	case 4:
		return "required"
	}
	return ""
}

func (*Ticker) SymbolId() uint16 {
	return 3
}

func (*Ticker) SymbolSinceVersion() uint16 {
	return 0
}

func (t *Ticker) SymbolInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SymbolSinceVersion()
}

func (*Ticker) SymbolDeprecated() uint16 {
	return 0
}

func (*Ticker) SymbolMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}
