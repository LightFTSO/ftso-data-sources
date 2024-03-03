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

type Trade struct {
	Timestamp uint64
	Symbol    Symbol
	Price     Decimal
	Size      Decimal
	Buy_side  uint8
	Source    []uint8
}

func (t *Trade) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
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
	if err := _m.WriteUint8(_w, t.Buy_side); err != nil {
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

func (t *Trade) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
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
	if !t.Buy_sideInActingVersion(actingVersion) {
		t.Buy_side = t.Buy_sideNullValue()
	} else {
		if err := _m.ReadUint8(_r, &t.Buy_side); err != nil {
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

func (t *Trade) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if t.TimestampInActingVersion(actingVersion) {
		if t.Timestamp < t.TimestampMinValue() || t.Timestamp > t.TimestampMaxValue() {
			return fmt.Errorf("Range check failed on t.Timestamp (%v < %v > %v)", t.TimestampMinValue(), t.Timestamp, t.TimestampMaxValue())
		}
	}
	if t.Buy_sideInActingVersion(actingVersion) {
		if t.Buy_side < t.Buy_sideMinValue() || t.Buy_side > t.Buy_sideMaxValue() {
			return fmt.Errorf("Range check failed on t.Buy_side (%v < %v > %v)", t.Buy_sideMinValue(), t.Buy_side, t.Buy_sideMaxValue())
		}
	}
	if !utf8.Valid(t.Source[:]) {
		return errors.New("t.Source failed UTF-8 validation")
	}
	return nil
}

func TradeInit(t *Trade) {
	return
}

func (*Trade) SbeBlockLength() (blockLength uint16) {
	return 39
}

func (*Trade) SbeTemplateId() (templateId uint16) {
	return 1
}

func (*Trade) SbeSchemaId() (schemaId uint16) {
	return 1
}

func (*Trade) SbeSchemaVersion() (schemaVersion uint16) {
	return 0
}

func (*Trade) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*Trade) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*Trade) TimestampId() uint16 {
	return 1
}

func (*Trade) TimestampSinceVersion() uint16 {
	return 0
}

func (t *Trade) TimestampInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.TimestampSinceVersion()
}

func (*Trade) TimestampDeprecated() uint16 {
	return 0
}

func (*Trade) TimestampMetaAttribute(meta int) string {
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

func (*Trade) TimestampMinValue() uint64 {
	return 0
}

func (*Trade) TimestampMaxValue() uint64 {
	return math.MaxUint64 - 1
}

func (*Trade) TimestampNullValue() uint64 {
	return math.MaxUint64
}

func (*Trade) SymbolId() uint16 {
	return 2
}

func (*Trade) SymbolSinceVersion() uint16 {
	return 0
}

func (t *Trade) SymbolInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SymbolSinceVersion()
}

func (*Trade) SymbolDeprecated() uint16 {
	return 0
}

func (*Trade) SymbolMetaAttribute(meta int) string {
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

func (*Trade) PriceId() uint16 {
	return 3
}

func (*Trade) PriceSinceVersion() uint16 {
	return 0
}

func (t *Trade) PriceInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.PriceSinceVersion()
}

func (*Trade) PriceDeprecated() uint16 {
	return 0
}

func (*Trade) PriceMetaAttribute(meta int) string {
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

func (*Trade) SizeId() uint16 {
	return 4
}

func (*Trade) SizeSinceVersion() uint16 {
	return 0
}

func (t *Trade) SizeInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SizeSinceVersion()
}

func (*Trade) SizeDeprecated() uint16 {
	return 0
}

func (*Trade) SizeMetaAttribute(meta int) string {
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

func (*Trade) Buy_sideId() uint16 {
	return 5
}

func (*Trade) Buy_sideSinceVersion() uint16 {
	return 0
}

func (t *Trade) Buy_sideInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.Buy_sideSinceVersion()
}

func (*Trade) Buy_sideDeprecated() uint16 {
	return 0
}

func (*Trade) Buy_sideMetaAttribute(meta int) string {
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

func (*Trade) Buy_sideMinValue() uint8 {
	return 0
}

func (*Trade) Buy_sideMaxValue() uint8 {
	return math.MaxUint8 - 1
}

func (*Trade) Buy_sideNullValue() uint8 {
	return math.MaxUint8
}

func (*Trade) SourceMetaAttribute(meta int) string {
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

func (*Trade) SourceSinceVersion() uint16 {
	return 0
}

func (t *Trade) SourceInActingVersion(actingVersion uint16) bool {
	return actingVersion >= t.SourceSinceVersion()
}

func (*Trade) SourceDeprecated() uint16 {
	return 0
}

func (Trade) SourceCharacterEncoding() string {
	return "UTF-8"
}

func (Trade) SourceHeaderLength() uint64 {
	return 4
}
