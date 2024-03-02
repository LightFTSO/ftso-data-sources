// Generated SBE (Simple Binary Encoding) message codec

package sbe

import (
	"fmt"
	"io"
	"math"
)

type VarStringEncoding struct {
	Length  uint32
	VarData uint8
}

func (v *VarStringEncoding) Encode(_m *SbeGoMarshaller, _w io.Writer) error {
	if err := _m.WriteUint32(_w, v.Length); err != nil {
		return err
	}
	if err := _m.WriteUint8(_w, v.VarData); err != nil {
		return err
	}
	return nil
}

func (v *VarStringEncoding) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16) error {
	if !v.LengthInActingVersion(actingVersion) {
		v.Length = v.LengthNullValue()
	} else {
		if err := _m.ReadUint32(_r, &v.Length); err != nil {
			return err
		}
	}
	if !v.VarDataInActingVersion(actingVersion) {
		v.VarData = v.VarDataNullValue()
	} else {
		if err := _m.ReadUint8(_r, &v.VarData); err != nil {
			return err
		}
	}
	return nil
}

func (v *VarStringEncoding) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if v.LengthInActingVersion(actingVersion) {
		if v.Length < v.LengthMinValue() || v.Length > v.LengthMaxValue() {
			return fmt.Errorf("Range check failed on v.Length (%v < %v > %v)", v.LengthMinValue(), v.Length, v.LengthMaxValue())
		}
	}
	if v.VarDataInActingVersion(actingVersion) {
		if v.VarData < v.VarDataMinValue() || v.VarData > v.VarDataMaxValue() {
			return fmt.Errorf("Range check failed on v.VarData (%v < %v > %v)", v.VarDataMinValue(), v.VarData, v.VarDataMaxValue())
		}
	}
	return nil
}

func VarStringEncodingInit(v *VarStringEncoding) {
	return
}

func (*VarStringEncoding) EncodedLength() int64 {
	return -1
}

func (*VarStringEncoding) LengthMinValue() uint32 {
	return 0
}

func (*VarStringEncoding) LengthMaxValue() uint32 {
	return 1073741824
}

func (*VarStringEncoding) LengthNullValue() uint32 {
	return math.MaxUint32
}

func (*VarStringEncoding) LengthSinceVersion() uint16 {
	return 0
}

func (v *VarStringEncoding) LengthInActingVersion(actingVersion uint16) bool {
	return actingVersion >= v.LengthSinceVersion()
}

func (*VarStringEncoding) LengthDeprecated() uint16 {
	return 0
}

func (*VarStringEncoding) VarDataMinValue() uint8 {
	return 0
}

func (*VarStringEncoding) VarDataMaxValue() uint8 {
	return math.MaxUint8 - 1
}

func (*VarStringEncoding) VarDataNullValue() uint8 {
	return math.MaxUint8
}

func (*VarStringEncoding) VarDataSinceVersion() uint16 {
	return 0
}

func (v *VarStringEncoding) VarDataInActingVersion(actingVersion uint16) bool {
	return actingVersion >= v.VarDataSinceVersion()
}

func (*VarStringEncoding) VarDataDeprecated() uint16 {
	return 0
}
