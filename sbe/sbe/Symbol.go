// Generated SBE (Simple Binary Encoding) message codec

package sbe

import (
	"fmt"
	"io"
)

type Symbol struct {
	Base  [6]byte
	Quote [6]byte
}

func (s *Symbol) Encode(_m *SbeGoMarshaller, _w io.Writer) error {
	if err := _m.WriteBytes(_w, s.Base[:]); err != nil {
		return err
	}
	if err := _m.WriteBytes(_w, s.Quote[:]); err != nil {
		return err
	}
	return nil
}

func (s *Symbol) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16) error {
	if !s.BaseInActingVersion(actingVersion) {
		for idx := 0; idx < 6; idx++ {
			s.Base[idx] = s.BaseNullValue()
		}
	} else {
		if err := _m.ReadBytes(_r, s.Base[:]); err != nil {
			return err
		}
	}
	if !s.QuoteInActingVersion(actingVersion) {
		for idx := 0; idx < 6; idx++ {
			s.Quote[idx] = s.QuoteNullValue()
		}
	} else {
		if err := _m.ReadBytes(_r, s.Quote[:]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Symbol) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if s.BaseInActingVersion(actingVersion) {
		for idx := 0; idx < 6; idx++ {
			if s.Base[idx] < s.BaseMinValue() || s.Base[idx] > s.BaseMaxValue() {
				return fmt.Errorf("Range check failed on s.Base[%d] (%v < %v > %v)", idx, s.BaseMinValue(), s.Base[idx], s.BaseMaxValue())
			}
		}
	}
	for idx, ch := range s.Base {
		if ch > 127 {
			return fmt.Errorf("s.Base[%d]=%d failed ASCII validation", idx, ch)
		}
	}
	if s.QuoteInActingVersion(actingVersion) {
		for idx := 0; idx < 6; idx++ {
			if s.Quote[idx] < s.QuoteMinValue() || s.Quote[idx] > s.QuoteMaxValue() {
				return fmt.Errorf("Range check failed on s.Quote[%d] (%v < %v > %v)", idx, s.QuoteMinValue(), s.Quote[idx], s.QuoteMaxValue())
			}
		}
	}
	for idx, ch := range s.Quote {
		if ch > 127 {
			return fmt.Errorf("s.Quote[%d]=%d failed ASCII validation", idx, ch)
		}
	}
	return nil
}

func SymbolInit(s *Symbol) {
	return
}

func (*Symbol) EncodedLength() int64 {
	return 12
}

func (*Symbol) BaseMinValue() byte {
	return byte(32)
}

func (*Symbol) BaseMaxValue() byte {
	return byte(126)
}

func (*Symbol) BaseNullValue() byte {
	return 0
}

func (s *Symbol) BaseCharacterEncoding() string {
	return "ASCII"
}

func (*Symbol) BaseSinceVersion() uint16 {
	return 0
}

func (s *Symbol) BaseInActingVersion(actingVersion uint16) bool {
	return actingVersion >= s.BaseSinceVersion()
}

func (*Symbol) BaseDeprecated() uint16 {
	return 0
}

func (*Symbol) QuoteMinValue() byte {
	return byte(32)
}

func (*Symbol) QuoteMaxValue() byte {
	return byte(126)
}

func (*Symbol) QuoteNullValue() byte {
	return 0
}

func (s *Symbol) QuoteCharacterEncoding() string {
	return "ASCII"
}

func (*Symbol) QuoteSinceVersion() uint16 {
	return 0
}

func (s *Symbol) QuoteInActingVersion(actingVersion uint16) bool {
	return actingVersion >= s.QuoteSinceVersion()
}

func (*Symbol) QuoteDeprecated() uint16 {
	return 0
}
