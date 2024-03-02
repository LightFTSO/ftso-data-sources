// Generated SBE (Simple Binary Encoding) message codec

package sbe

import (
	"fmt"
	"io"
	"math"
)

type Decimal struct {
	Mantissa uint64
	Exponent int8
}

func (d *Decimal) Encode(_m *SbeGoMarshaller, _w io.Writer) error {
	if err := _m.WriteUint64(_w, d.Mantissa); err != nil {
		return err
	}
	if err := _m.WriteInt8(_w, d.Exponent); err != nil {
		return err
	}
	return nil
}

func (d *Decimal) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16) error {
	if !d.MantissaInActingVersion(actingVersion) {
		d.Mantissa = d.MantissaNullValue()
	} else {
		if err := _m.ReadUint64(_r, &d.Mantissa); err != nil {
			return err
		}
	}
	if !d.ExponentInActingVersion(actingVersion) {
		d.Exponent = d.ExponentNullValue()
	} else {
		if err := _m.ReadInt8(_r, &d.Exponent); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decimal) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if d.MantissaInActingVersion(actingVersion) {
		if d.Mantissa < d.MantissaMinValue() || d.Mantissa > d.MantissaMaxValue() {
			return fmt.Errorf("Range check failed on d.Mantissa (%v < %v > %v)", d.MantissaMinValue(), d.Mantissa, d.MantissaMaxValue())
		}
	}
	if d.ExponentInActingVersion(actingVersion) {
		if d.Exponent < d.ExponentMinValue() || d.Exponent > d.ExponentMaxValue() {
			return fmt.Errorf("Range check failed on d.Exponent (%v < %v > %v)", d.ExponentMinValue(), d.Exponent, d.ExponentMaxValue())
		}
	}
	return nil
}

func DecimalInit(d *Decimal) {
	return
}

func (*Decimal) EncodedLength() int64 {
	return 9
}

func (*Decimal) MantissaMinValue() uint64 {
	return 0
}

func (*Decimal) MantissaMaxValue() uint64 {
	return math.MaxUint64 - 1
}

func (*Decimal) MantissaNullValue() uint64 {
	return math.MaxUint64
}

func (*Decimal) MantissaSinceVersion() uint16 {
	return 0
}

func (d *Decimal) MantissaInActingVersion(actingVersion uint16) bool {
	return actingVersion >= d.MantissaSinceVersion()
}

func (*Decimal) MantissaDeprecated() uint16 {
	return 0
}

func (*Decimal) ExponentMinValue() int8 {
	return math.MinInt8 + 1
}

func (*Decimal) ExponentMaxValue() int8 {
	return math.MaxInt8
}

func (*Decimal) ExponentNullValue() int8 {
	return math.MinInt8
}

func (*Decimal) ExponentSinceVersion() uint16 {
	return 0
}

func (d *Decimal) ExponentInActingVersion(actingVersion uint16) bool {
	return actingVersion >= d.ExponentSinceVersion()
}

func (*Decimal) ExponentDeprecated() uint16 {
	return 0
}
