package protocol

import (
	"hash/crc32"
	"testing"
)

func TestBuildRecordBatchRoundTrips(t *testing.T) {
	records := []BatchRecord{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: nil, Value: []byte("v1")},
	}
	batch := BuildRecordBatch(records)

	if batch[16] != 2 {
		t.Fatalf("magic = %d, want 2", batch[16])
	}
	wantCRC := crc32.Checksum(batch[21:], castagnoliTable)
	if got := uint32(batch[17])<<24 | uint32(batch[18])<<16 | uint32(batch[19])<<8 | uint32(batch[20]); got != wantCRC {
		t.Fatalf("crc = %d, want %d", got, wantCRC)
	}

	decoded, err := DecodeRecordBatches(batch)
	if err != nil {
		t.Fatalf("DecodeRecordBatches: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded %d records, want 2", len(decoded))
	}
	if decoded[0].Offset != 0 || string(decoded[0].Key) != "k0" || string(decoded[0].Value) != "v0" {
		t.Fatalf("record 0 = %+v", decoded[0])
	}
	if decoded[1].Offset != 1 || decoded[1].Key != nil || string(decoded[1].Value) != "v1" {
		t.Fatalf("record 1 = %+v", decoded[1])
	}
	if decoded[0].Timestamp != baseTimestamp {
		t.Fatalf("timestamp = %d, want %d", decoded[0].Timestamp, baseTimestamp)
	}
}

func TestDecodeRecordBatchesResolvesOffsetAcrossBatches(t *testing.T) {
	first := BuildRecordBatch([]BatchRecord{{Value: []byte("a")}})
	setBaseOffset(first, 0)
	second := BuildRecordBatch([]BatchRecord{{Value: []byte("b")}})
	setBaseOffset(second, 1)

	decoded, err := DecodeRecordBatches(append(append([]byte{}, first...), second...))
	if err != nil {
		t.Fatalf("DecodeRecordBatches: %v", err)
	}
	if len(decoded) != 2 || decoded[0].Offset != 0 || decoded[1].Offset != 1 {
		t.Fatalf("offsets = %d, %d; want 0, 1", decoded[0].Offset, decoded[1].Offset)
	}
	if string(decoded[0].Value) != "a" || string(decoded[1].Value) != "b" {
		t.Fatalf("values = %q, %q", decoded[0].Value, decoded[1].Value)
	}
}

func setBaseOffset(batch []byte, offset uint64) {
	batch[0] = byte(offset >> 56)
	batch[1] = byte(offset >> 48)
	batch[2] = byte(offset >> 40)
	batch[3] = byte(offset >> 32)
	batch[4] = byte(offset >> 24)
	batch[5] = byte(offset >> 16)
	batch[6] = byte(offset >> 8)
	batch[7] = byte(offset)
}
