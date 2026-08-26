package protocol

import (
	"bytes"
	"testing"
)

func TestInt8RoundTrip(t *testing.T) {
	for _, v := range []int8{0, 1, -1, 127, -128} {
		e := NewEncoder()
		e.WriteInt8(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadInt8()
		if err != nil {
			t.Fatalf("ReadInt8(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadInt8(%d): got %d", v, got)
		}
	}
}

func TestInt16RoundTrip(t *testing.T) {
	for _, v := range []int16{0, 1, -1, 32767, -32768} {
		e := NewEncoder()
		e.WriteInt16(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadInt16()
		if err != nil {
			t.Fatalf("ReadInt16(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadInt16(%d): got %d", v, got)
		}
	}
}

func TestInt32RoundTrip(t *testing.T) {
	for _, v := range []int32{0, 1, -1, 2147483647, -2147483648} {
		e := NewEncoder()
		e.WriteInt32(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadInt32()
		if err != nil {
			t.Fatalf("ReadInt32(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadInt32(%d): got %d", v, got)
		}
	}
}

func TestInt64RoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 9223372036854775807, -9223372036854775808} {
		e := NewEncoder()
		e.WriteInt64(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadInt64()
		if err != nil {
			t.Fatalf("ReadInt64(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadInt64(%d): got %d", v, got)
		}
	}
}

// TestUnsignedVarintExactBytes checks against hand-derived worked examples
// for the base-128 varint encoding, so a bug that's merely self-consistent
// between WriteUnsignedVarint and ReadUnsignedVarint doesn't slip through.
func TestUnsignedVarintExactBytes(t *testing.T) {
	cases := []struct {
		value uint32
		want  []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xAC, 0x02}},
	}
	for _, c := range cases {
		e := NewEncoder()
		e.WriteUnsignedVarint(c.value)
		if !bytes.Equal(e.Bytes(), c.want) {
			t.Errorf("WriteUnsignedVarint(%d): got % x, want % x", c.value, e.Bytes(), c.want)
		}

		got, err := NewDecoder(bytes.NewReader(c.want)).ReadUnsignedVarint()
		if err != nil {
			t.Fatalf("ReadUnsignedVarint(% x): unexpected error: %v", c.want, err)
		}
		if got != c.value {
			t.Errorf("ReadUnsignedVarint(% x): got %d, want %d", c.want, got, c.value)
		}
	}
}

// TestVarintExactBytes checks the zigzag+varint encoding against hand-derived
// worked examples (same scheme as Protocol Buffers, which the Kafka guide
// documents).
func TestVarintExactBytes(t *testing.T) {
	cases := []struct {
		value int32
		want  []byte
	}{
		{0, []byte{0x00}},
		{-1, []byte{0x01}},
		{1, []byte{0x02}},
		{-2, []byte{0x03}},
		{2, []byte{0x04}},
		{63, []byte{0x7E}},
		{-64, []byte{0x7F}},
		{64, []byte{0x80, 0x01}},
		{-65, []byte{0x81, 0x01}},
	}
	for _, c := range cases {
		e := NewEncoder()
		e.WriteVarint(c.value)
		if !bytes.Equal(e.Bytes(), c.want) {
			t.Errorf("WriteVarint(%d): got % x, want % x", c.value, e.Bytes(), c.want)
		}

		got, err := NewDecoder(bytes.NewReader(c.want)).ReadVarint()
		if err != nil {
			t.Fatalf("ReadVarint(% x): unexpected error: %v", c.want, err)
		}
		if got != c.value {
			t.Errorf("ReadVarint(% x): got %d, want %d", c.want, got, c.value)
		}
	}
}

func TestReadUnsignedVarintTooLong(t *testing.T) {
	// 5 bytes, every one with the continuation bit set — no terminating byte
	// within the maximum length for a 32-bit varint.
	malformed := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := NewDecoder(bytes.NewReader(malformed)).ReadUnsignedVarint(); err == nil {
		t.Fatal("expected an error for an over-length varint, got nil")
	}
}

func TestStringRoundTrip(t *testing.T) {
	for _, v := range []string{"", "hello", "unicode: héllo wörld"} {
		e := NewEncoder()
		e.WriteString(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadString()
		if err != nil {
			t.Fatalf("ReadString(%q): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadString(%q): got %q", v, got)
		}
	}
}

func TestNullableStringRoundTrip(t *testing.T) {
	hello := "hello"
	empty := ""
	for _, v := range []*string{nil, &hello, &empty} {
		e := NewEncoder()
		e.WriteNullableString(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadNullableString()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if (v == nil) != (got == nil) {
			t.Fatalf("nullability mismatch: got %v, want %v", got, v)
		}
		if v != nil && *got != *v {
			t.Errorf("got %q, want %q", *got, *v)
		}
	}
}

func TestCompactStringRoundTrip(t *testing.T) {
	for _, v := range []string{"", "hello", "unicode: héllo wörld"} {
		e := NewEncoder()
		e.WriteCompactString(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadCompactString()
		if err != nil {
			t.Fatalf("ReadCompactString(%q): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("ReadCompactString(%q): got %q", v, got)
		}
	}
}

func TestCompactNullableStringRoundTrip(t *testing.T) {
	hello := "hello"
	empty := ""
	for _, v := range []*string{nil, &hello, &empty} {
		e := NewEncoder()
		e.WriteCompactNullableString(v)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadCompactNullableString()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if (v == nil) != (got == nil) {
			t.Fatalf("nullability mismatch: got %v, want %v", got, v)
		}
		if v != nil && *got != *v {
			t.Errorf("got %q, want %q", *got, *v)
		}
	}
}

func TestArrayLenRoundTrip(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 100} {
		e := NewEncoder()
		e.WriteArrayLen(n)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadArrayLen()
		if err != nil {
			t.Fatalf("ReadArrayLen(%d): unexpected error: %v", n, err)
		}
		if got != n {
			t.Errorf("ReadArrayLen(%d): got %d", n, got)
		}
	}
}

func TestCompactArrayLenRoundTrip(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 100} {
		e := NewEncoder()
		e.WriteCompactArrayLen(n)
		got, err := NewDecoder(bytes.NewReader(e.Bytes())).ReadCompactArrayLen()
		if err != nil {
			t.Fatalf("ReadCompactArrayLen(%d): unexpected error: %v", n, err)
		}
		if got != n {
			t.Errorf("ReadCompactArrayLen(%d): got %d", n, got)
		}
	}
}

func TestEmptyTagBufferRoundTrip(t *testing.T) {
	e := NewEncoder()
	e.WriteEmptyTagBuffer()
	if err := NewDecoder(bytes.NewReader(e.Bytes())).ReadTagBuffer(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadTagBufferRejectsNonEmpty(t *testing.T) {
	e := NewEncoder()
	e.WriteUnsignedVarint(2) // pretend there are 2 tagged fields
	if err := NewDecoder(bytes.NewReader(e.Bytes())).ReadTagBuffer(); err == nil {
		t.Fatal("expected an error for a non-empty tag buffer, got nil")
	}
}
