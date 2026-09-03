package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// recordBatchHeaderLen is the fixed size of a v2 RecordBatch header, up to and
// including the record_count field.
const recordBatchHeaderLen = 61

// baseTimestamp is a fixed epoch-millis value stamped into every batch this
// codec builds. Nothing in the broker derives behaviour from it yet.
const baseTimestamp int64 = 1700000000000

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// BatchRecord is one record to encode into a RecordBatch. A nil Key encodes as
// a null key.
type BatchRecord struct {
	Key   []byte
	Value []byte
}

// DecodedRecord is one record read back out of a RecordBatch, with its absolute
// offset and timestamp resolved from the batch header deltas.
type DecodedRecord struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}

// BuildRecordBatch encodes records into a single non-idempotent v2 RecordBatch.
// The base offset is written as 0; whoever appends the batch stamps the real
// base offset into bytes [0:8].
func BuildRecordBatch(records []BatchRecord) []byte {
	var body bytes.Buffer
	for i, r := range records {
		body.Write(encodeRecord(r, int64(i)))
	}

	batch := make([]byte, recordBatchHeaderLen, recordBatchHeaderLen+body.Len())
	binary.BigEndian.PutUint64(batch[0:8], 0)                        // base offset
	binary.BigEndian.PutUint32(batch[12:16], ^uint32(0))             // partition leader epoch = -1
	batch[16] = 2                                                    // magic
	binary.BigEndian.PutUint16(batch[21:23], 0)                      // attributes
	binary.BigEndian.PutUint32(batch[23:27], uint32(len(records)-1)) // last offset delta
	binary.BigEndian.PutUint64(batch[27:35], uint64(baseTimestamp))  // base timestamp
	binary.BigEndian.PutUint64(batch[35:43], uint64(baseTimestamp))  // max timestamp
	binary.BigEndian.PutUint64(batch[43:51], ^uint64(0))             // producer id = -1
	binary.BigEndian.PutUint16(batch[51:53], ^uint16(0))             // producer epoch = -1
	binary.BigEndian.PutUint32(batch[53:57], ^uint32(0))             // base sequence = -1
	binary.BigEndian.PutUint32(batch[57:61], uint32(len(records)))   // record count
	batch = append(batch, body.Bytes()...)

	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-12)) // batch length = bytes after this field
	binary.BigEndian.PutUint32(batch[17:21], crc32.Checksum(batch[21:], castagnoliTable))
	return batch
}

// DecodeRecordBatches walks a concatenation of v2 RecordBatches (as returned by
// Fetch) and returns every record, offsets and timestamps resolved.
func DecodeRecordBatches(raw []byte) ([]DecodedRecord, error) {
	var out []DecodedRecord
	for len(raw) >= 12 {
		batchLen := int(binary.BigEndian.Uint32(raw[8:12]))
		total := 12 + batchLen
		if total < recordBatchHeaderLen || total > len(raw) {
			break
		}
		records, err := decodeOneBatch(raw[:total])
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
		raw = raw[total:]
	}
	return out, nil
}

func decodeOneBatch(batch []byte) ([]DecodedRecord, error) {
	if len(batch) < recordBatchHeaderLen {
		return nil, fmt.Errorf("protocol: record batch shorter than header")
	}
	if batch[16] != 2 {
		return nil, fmt.Errorf("protocol: unsupported record batch magic %d", batch[16])
	}
	baseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
	baseTS := int64(binary.BigEndian.Uint64(batch[27:35]))
	recordCount := int32(binary.BigEndian.Uint32(batch[57:61]))

	r := bytes.NewReader(batch[recordBatchHeaderLen:])
	dec := NewDecoder(r)
	out := make([]DecodedRecord, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		record, err := decodeRecord(dec, r, baseOffset, baseTS)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func encodeRecord(r BatchRecord, offsetDelta int64) []byte {
	var body bytes.Buffer

	head := &Encoder{}
	head.WriteInt8(0)                    // record attributes
	head.WriteVarint(0)                  // timestamp delta
	head.WriteVarint(int32(offsetDelta)) // offset delta
	if r.Key == nil {
		head.WriteVarint(-1)
	} else {
		head.WriteVarint(int32(len(r.Key)))
	}
	body.Write(head.Bytes())
	body.Write(r.Key)

	val := &Encoder{}
	val.WriteVarint(int32(len(r.Value)))
	body.Write(val.Bytes())
	body.Write(r.Value)

	tail := &Encoder{}
	tail.WriteVarint(0) // header count
	body.Write(tail.Bytes())

	length := &Encoder{}
	length.WriteVarint(int32(body.Len()))
	return append(length.Bytes(), body.Bytes()...)
}

func decodeRecord(dec *Decoder, r io.Reader, baseOffset, baseTS int64) (DecodedRecord, error) {
	if _, err := dec.ReadVarint(); err != nil { // record length
		return DecodedRecord{}, err
	}
	if _, err := dec.ReadInt8(); err != nil { // attributes
		return DecodedRecord{}, err
	}
	tsDelta, err := dec.ReadVarint()
	if err != nil {
		return DecodedRecord{}, err
	}
	offsetDelta, err := dec.ReadVarint()
	if err != nil {
		return DecodedRecord{}, err
	}
	key, err := readVarintBytes(dec, r)
	if err != nil {
		return DecodedRecord{}, err
	}
	value, err := readVarintBytes(dec, r)
	if err != nil {
		return DecodedRecord{}, err
	}
	if _, err := dec.ReadVarint(); err != nil { // header count
		return DecodedRecord{}, err
	}
	return DecodedRecord{
		Offset:    baseOffset + int64(offsetDelta),
		Key:       key,
		Value:     value,
		Timestamp: baseTS + int64(tsDelta),
	}, nil
}

// readVarintBytes reads a varint length then that many raw bytes. A length of
// -1 returns nil.
func readVarintBytes(dec *Decoder, r io.Reader) ([]byte, error) {
	n, err := dec.ReadVarint()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
