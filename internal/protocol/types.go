// Package protocol implements encode/decode for Kafka wire-protocol
// primitive types, as defined by the "Protocol Primitive Types" section of
// the Kafka Protocol Guide (https://kafka.apache.org/protocol.html).
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// maxVarintBytes is the most bytes a 32-bit base-128 varint can occupy:
// ceil(32/7) = 5.
const maxVarintBytes = 5

var errVarintTooLong = errors.New("protocol: varint exceeds maximum length")

// Encoder writes Kafka wire-protocol primitive types into an internal
// buffer. Use Bytes to get the accumulated output.
type Encoder struct {
	buf bytes.Buffer
}

// NewEncoder returns an Encoder ready to write into.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Bytes returns the bytes written so far.
func (e *Encoder) Bytes() []byte {
	return e.buf.Bytes()
}

// WriteInt8 writes a single signed byte.
func (e *Encoder) WriteInt8(v int8) {
	e.buf.WriteByte(byte(v))
}

// WriteInt16 writes a big-endian signed 16-bit integer.
func (e *Encoder) WriteInt16(v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	e.buf.Write(b[:])
}

// WriteInt32 writes a big-endian signed 32-bit integer.
func (e *Encoder) WriteInt32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	e.buf.Write(b[:])
}

// WriteInt64 writes a big-endian signed 64-bit integer.
func (e *Encoder) WriteInt64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	e.buf.Write(b[:])
}

// WriteUnsignedVarint writes v using base-128 varint encoding: 7 data bits
// per byte, least-significant group first, with the top bit of every byte
// except the last set to 1 to signal "more bytes follow."
func (e *Encoder) WriteUnsignedVarint(v uint32) {
	for v >= 0x80 {
		e.buf.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	e.buf.WriteByte(byte(v))
}

// WriteVarint zigzag-encodes v so that small negative numbers stay small,
// then writes the result as an unsigned varint.
func (e *Encoder) WriteVarint(v int32) {
	zigzag := uint32((v << 1) ^ (v >> 31))
	e.WriteUnsignedVarint(zigzag)
}

// WriteString writes a non-nullable STRING: an INT16 length prefix followed
// by the raw UTF-8 bytes.
func (e *Encoder) WriteString(s string) {
	e.WriteInt16(int16(len(s)))
	e.buf.WriteString(s)
}

// WriteNullableString writes s, or an INT16 length of -1 if s is nil.
func (e *Encoder) WriteNullableString(s *string) {
	if s == nil {
		e.WriteInt16(-1)
		return
	}
	e.WriteString(*s)
}

// WriteCompactString writes a non-nullable COMPACT_STRING: an unsigned
// varint length of len(s)+1, followed by the raw UTF-8 bytes.
func (e *Encoder) WriteCompactString(s string) {
	e.WriteUnsignedVarint(uint32(len(s)) + 1)
	e.buf.WriteString(s)
}

// WriteCompactNullableString writes s using the compact encoding, or an
// unsigned varint 0 if s is nil.
func (e *Encoder) WriteCompactNullableString(s *string) {
	if s == nil {
		e.WriteUnsignedVarint(0)
		return
	}
	e.WriteCompactString(*s)
}

// WriteArrayLen writes a classic ARRAY length prefix: an INT32 of n, or -1
// for a null array.
func (e *Encoder) WriteArrayLen(n int) {
	e.WriteInt32(int32(n))
}

// WriteCompactArrayLen writes a COMPACT_ARRAY length prefix: an unsigned
// varint of n+1, or 0 for a null array (n == -1).
func (e *Encoder) WriteCompactArrayLen(n int) {
	e.WriteUnsignedVarint(uint32(n + 1))
}

// WriteEmptyTagBuffer writes a TAG_BUFFER with zero tagged fields.
func (e *Encoder) WriteEmptyTagBuffer() {
	e.WriteUnsignedVarint(0)
}

// Decoder reads Kafka wire-protocol primitive types from an io.Reader.
type Decoder struct {
	r io.Reader
}

// NewDecoder returns a Decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) readByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(d.r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadInt8 reads a single signed byte.
func (d *Decoder) ReadInt8() (int8, error) {
	b, err := d.readByte()
	if err != nil {
		return 0, err
	}
	return int8(b), nil
}

// ReadInt16 reads a big-endian signed 16-bit integer.
func (d *Decoder) ReadInt16() (int16, error) {
	var b [2]byte
	if _, err := io.ReadFull(d.r, b[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b[:])), nil
}

// ReadInt32 reads a big-endian signed 32-bit integer.
func (d *Decoder) ReadInt32() (int32, error) {
	var b [4]byte
	if _, err := io.ReadFull(d.r, b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}

// ReadInt64 reads a big-endian signed 64-bit integer.
func (d *Decoder) ReadInt64() (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(d.r, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

// ReadUnsignedVarint reads a base-128 varint (see WriteUnsignedVarint for the
// encoding). It returns an error if more than 5 bytes are consumed without
// finding a terminating byte, since that can't be a valid 32-bit value.
func (d *Decoder) ReadUnsignedVarint() (uint32, error) {
	var result uint32
	var shift uint
	for i := 0; i < maxVarintBytes; i++ {
		b, err := d.readByte()
		if err != nil {
			return 0, err
		}
		result |= uint32(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
	}
	return 0, errVarintTooLong
}

// ReadVarint reads an unsigned varint and reverses the zigzag transform
// applied by WriteVarint.
func (d *Decoder) ReadVarint() (int32, error) {
	zigzag, err := d.ReadUnsignedVarint()
	if err != nil {
		return 0, err
	}
	return int32(zigzag>>1) ^ -int32(zigzag&1), nil
}

// ReadString reads a non-nullable STRING.
func (d *Decoder) ReadString() (string, error) {
	n, err := d.ReadInt16()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", fmt.Errorf("protocol: negative length %d for non-nullable string", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// ReadNullableString reads a NULLABLE_STRING, returning nil if the encoded
// length was -1.
func (d *Decoder) ReadNullableString() (*string, error) {
	n, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}
	s := string(buf)
	return &s, nil
}

// ReadCompactString reads a non-nullable COMPACT_STRING.
func (d *Decoder) ReadCompactString() (string, error) {
	n, err := d.ReadUnsignedVarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", errors.New("protocol: zero length for non-nullable compact string")
	}
	buf := make([]byte, n-1)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// ReadCompactNullableString reads a COMPACT_NULLABLE_STRING, returning nil
// if the encoded length was 0.
func (d *Decoder) ReadCompactNullableString() (*string, error) {
	n, err := d.ReadUnsignedVarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n-1)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}
	s := string(buf)
	return &s, nil
}

// ReadArrayLen reads a classic ARRAY length prefix, returning -1 for a null
// array.
func (d *Decoder) ReadArrayLen() (int, error) {
	n, err := d.ReadInt32()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ReadCompactArrayLen reads a COMPACT_ARRAY length prefix, returning -1 for
// a null array.
func (d *Decoder) ReadCompactArrayLen() (int, error) {
	n, err := d.ReadUnsignedVarint()
	if err != nil {
		return 0, err
	}
	return int(n) - 1, nil
}

// ReadTagBuffer reads a TAG_BUFFER. Only the empty case (zero tagged fields)
// is supported for now; real tagged-field parsing isn't needed until a later
// phase actually uses flexible versions with real tags.
func (d *Decoder) ReadTagBuffer() error {
	n, err := d.ReadUnsignedVarint()
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("protocol: tagged fields not supported (count=%d)", n)
	}
	return nil
}
