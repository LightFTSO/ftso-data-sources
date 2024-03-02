// Generated SBE (Simple Binary Encoding) message codec

package sbe

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"unicode/utf8"
)

type Ticker struct {
	Timestamp uint64
	Symbol    Symbol
	Price     Decimal
	Size      Decimal
	Side_sell BooleanTypeEnum
	Source    []uint8
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
	if err := t.Symbol.Encode(_m, _w); err != nil {
		return err
	}
	if err := t.Price.Encode(_m, _w); err != nil {
		return err
	}
	if err := t.Size.Encode(_m, _w); err != nil {
		return err
	}
	if err := t.Side_sell.Encode(_m, _w); err != nil {
		return err
	}
	if err := _m.WriteUint32(_w, uint32(len(t.Source))); err != nil {
		return err
	}
	if err := _m.WriteBytes(_w, t.Source); err != nil {
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
	if t.SymbolInActingVersion(actingVersion) {
		if err := t.Symbol.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if t.PriceInActingVersion(actingVersion) {
		if err := t.Price.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if t.SizeInActingVersion(actingVersion) {
		if err := t.Size.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if t.Side_sellInActingVersion(actingVersion) {
		if err := t.Side_sell.Decode(_m, _r, actingVersion); err != nil {
			return err
		}
	}
	if actingVersion > t.SbeSchemaVersion() && blockLength > t.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-t.SbeBlockLength()))
	}

	if t.SourceInActingVersion(actingVersion) {
		var SourceLength uint32
		if err := _m.ReadUint32(_r, &SourceLength); err != nil {
			return err
		}
		if cap(t.Source) < int(SourceLength) {
			t.Source = make([]uint8, SourceLength)
		}
		t.Source = t.Source[:SourceLength]
		if err := _m.ReadBytes(_r, t.Source); err != nil {
			return err
		}
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
	if err := t.Side_sell.RangeCheck(actingVersion, schemaVersion); err != nil {
		return err
	}
	if !utf8.Valid(t.Source[:]) {
		return errors.New("t.Source failed UTF-8 validation")
	}
	return nil
}

func TickerInit(t *Ticker) {
	return
}

func (*Ticker) SbeBlockLength() (blockLength uint16) {
	return 39
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

func (*Ticker) SymbolId() uint16 {
	return 2
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

func (*Ticker) PriceId() uint16 {
	return 3
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
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*Ticker) SizeId() uint16 {
	return 4
}

func (*Ticker) SizeSinceVersion() uint16 {
	return 0
}

func (t *Ticker) SizeInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SizeSinceVersion()
}

func (*Ticker) SizeDeprecated() uint16 {
	return 0
}

func (*Ticker) SizeMetaAttribute(meta int) string {
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

func (*Ticker) Side_sellId() uint16 {
	return 5
}

func (*Ticker) Side_sellSinceVersion() uint16 {
	return 0
}

func (t *Ticker) Side_sellInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.Side_sellSinceVersion()
}

func (*Ticker) Side_sellDeprecated() uint16 {
	return 0
}

func (*Ticker) Side_sellMetaAttribute(meta int) string {
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

func (*Ticker) SourceMetaAttribute(meta int) string {
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

func (*Ticker) SourceSinceVersion() uint16 {
	return 0
}

func (t *Ticker) SourceInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SourceSinceVersion()
}

func (*Ticker) SourceDeprecated() uint16 {
	return 0
}

func (Ticker) SourceCharacterEncoding() string {
	return "UTF-8"
}

func (Ticker) SourceHeaderLength() uint64 {
	return 4
}
