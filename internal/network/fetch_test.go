package network

import (
	"bytes"
	"io"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleFetchReturnsRecordBatchesAndHighWatermark(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	produceTestMessages(t, broker, "one", "two", "three")

	body, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("orders", 0, 0, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch: unexpected error: %v", err)
	}

	values, highWatermark := assertFetchResult(t, body, "orders", 0, protocol.ErrNone)
	if highWatermark != 3 {
		t.Fatalf("high watermark: got %d, want 3", highWatermark)
	}
	assertValues(t, values, []string{"one", "two", "three"})
}

func TestHandleFetchFromOffsetReturnsRemainingBatches(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	produceTestMessages(t, broker, "one", "two", "three")

	body, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("orders", 0, 1, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch: unexpected error: %v", err)
	}

	values, highWatermark := assertFetchResult(t, body, "orders", 0, protocol.ErrNone)
	if highWatermark != 3 {
		t.Fatalf("high watermark: got %d, want 3", highWatermark)
	}
	assertValues(t, values, []string{"two", "three"})
}

func TestHandleFetchAtEndReturnsEmptyRecords(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	produceTestMessages(t, broker, "one", "two", "three")

	body, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("orders", 0, 3, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch: unexpected error: %v", err)
	}

	values, highWatermark := assertFetchResult(t, body, "orders", 0, protocol.ErrNone)
	if highWatermark != 3 {
		t.Fatalf("high watermark: got %d, want 3", highWatermark)
	}
	assertValues(t, values, nil)
}

func TestHandleFetchRejectsOffsetOutOfRange(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	produceTestMessages(t, broker, "one", "two", "three")

	body, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("orders", 0, 4, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch: unexpected error: %v", err)
	}

	values, highWatermark := assertFetchResult(t, body, "orders", 0, protocol.ErrOffsetOutOfRange)
	if highWatermark != 3 {
		t.Fatalf("high watermark: got %d, want 3", highWatermark)
	}
	assertValues(t, values, nil)
}

func TestHandleFetchRejectsUnknownTopicOrPartition(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())

	unknownTopic, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("ghost", 0, 0, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch unknown topic: unexpected error: %v", err)
	}
	unknownPartition, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("orders", 1, 0, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch unknown partition: unexpected error: %v", err)
	}

	values, highWatermark := assertFetchResult(t, unknownTopic, "ghost", 0, protocol.ErrUnknownTopicOrPartition)
	if highWatermark != 0 {
		t.Fatalf("unknown topic high watermark: got %d, want 0", highWatermark)
	}
	assertValues(t, values, nil)

	values, highWatermark = assertFetchResult(t, unknownPartition, "orders", 1, protocol.ErrUnknownTopicOrPartition)
	if highWatermark != 0 {
		t.Fatalf("unknown partition high watermark: got %d, want 0", highWatermark)
	}
	assertValues(t, values, nil)
}

func TestFetchHandlerIsRegistered(t *testing.T) {
	if dispatchTable[1] == nil {
		t.Fatal("Fetch handler for api_key 1 is not registered")
	}
}

func produceTestMessages(t *testing.T, broker *Broker, values ...string) {
	t.Helper()

	for _, value := range values {
		if _, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, buildRecordBatch(t, value))); err != nil {
			t.Fatalf("handleProduce: unexpected error: %v", err)
		}
	}
}

func fetchRequest(topicName string, partition int32, offset int64, maxBytes int32) []byte {
	e := protocol.NewEncoder()
	e.WriteInt32(-1)
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(offset)
	e.WriteInt32(maxBytes)
	return e.Bytes()
}

func assertFetchResult(t *testing.T, body []byte, wantTopic string, wantPartition int32, wantCode int16) ([]string, int64) {
	t.Helper()

	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen fetch topics: unexpected error: %v", err)
	}
	if topicCount != 1 {
		t.Fatalf("fetch topic count: got %d, want 1", topicCount)
	}
	topicName, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen fetch partitions: unexpected error: %v", err)
	}
	if topicName != wantTopic || partitionCount != 1 {
		t.Fatalf("fetch topic response: got {%q, partitions=%d}, want {%q, partitions=1}", topicName, partitionCount, wantTopic)
	}

	partition, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 partition: unexpected error: %v", err)
	}
	code, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	highWatermark, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 high watermark: unexpected error: %v", err)
	}
	recordSet, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes records: unexpected error: %v", err)
	}
	if partition != wantPartition || code != wantCode {
		t.Fatalf("fetch partition response: got {partition=%d, code=%d}, want {%d, %d}", partition, code, wantPartition, wantCode)
	}

	return decodeRecordSet(t, recordSet), highWatermark
}

func decodeRecordSet(t *testing.T, recordSet []byte) []string {
	t.Helper()

	var values []string
	reader := bytes.NewReader(recordSet)
	for reader.Len() > 0 {
		batchStart := int64(len(recordSet) - reader.Len())
		dec := protocol.NewDecoder(reader)
		if _, err := dec.ReadInt64(); err != nil {
			t.Fatalf("ReadInt64 base offset: unexpected error: %v", err)
		}
		batchLength, err := dec.ReadInt32()
		if err != nil {
			t.Fatalf("ReadInt32 batch length: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 partition leader epoch: unexpected error: %v", err)
		}
		magic, err := dec.ReadInt8()
		if err != nil {
			t.Fatalf("ReadInt8 magic: unexpected error: %v", err)
		}
		if magic != 2 {
			t.Fatalf("magic: got %d, want 2", magic)
		}
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 crc: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt16(); err != nil {
			t.Fatalf("ReadInt16 attributes: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 last offset delta: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt64(); err != nil {
			t.Fatalf("ReadInt64 base timestamp: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt64(); err != nil {
			t.Fatalf("ReadInt64 max timestamp: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt64(); err != nil {
			t.Fatalf("ReadInt64 producer id: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt16(); err != nil {
			t.Fatalf("ReadInt16 producer epoch: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 base sequence: unexpected error: %v", err)
		}
		recordCount, err := dec.ReadInt32()
		if err != nil {
			t.Fatalf("ReadInt32 record count: unexpected error: %v", err)
		}
		for i := 0; i < int(recordCount); i++ {
			value := decodeRecordValue(t, reader, dec)
			values = append(values, value)
		}

		nextBatch := batchStart + 12 + int64(batchLength)
		if _, err := reader.Seek(nextBatch, io.SeekStart); err != nil {
			t.Fatalf("seek next batch: unexpected error: %v", err)
		}
	}

	return values
}

func decodeRecordValue(t *testing.T, reader *bytes.Reader, dec *protocol.Decoder) string {
	t.Helper()

	if _, err := dec.ReadVarint(); err != nil {
		t.Fatalf("ReadVarint record length: unexpected error: %v", err)
	}
	if _, err := dec.ReadInt8(); err != nil {
		t.Fatalf("ReadInt8 record attributes: unexpected error: %v", err)
	}
	if _, err := dec.ReadVarint(); err != nil {
		t.Fatalf("ReadVarint timestamp delta: unexpected error: %v", err)
	}
	if _, err := dec.ReadVarint(); err != nil {
		t.Fatalf("ReadVarint offset delta: unexpected error: %v", err)
	}
	keyLength, err := dec.ReadVarint()
	if err != nil {
		t.Fatalf("ReadVarint key length: unexpected error: %v", err)
	}
	if keyLength > 0 {
		key := make([]byte, keyLength)
		if _, err := io.ReadFull(reader, key); err != nil {
			t.Fatalf("Read key: unexpected error: %v", err)
		}
	}
	valueLength, err := dec.ReadVarint()
	if err != nil {
		t.Fatalf("ReadVarint value length: unexpected error: %v", err)
	}
	value := make([]byte, valueLength)
	if _, err := io.ReadFull(reader, value); err != nil {
		t.Fatalf("Read value: unexpected error: %v", err)
	}
	if _, err := dec.ReadVarint(); err != nil {
		t.Fatalf("ReadVarint header count: unexpected error: %v", err)
	}
	return string(value)
}

func assertValues(t *testing.T, got []string, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("values count: got %d %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("value %d: got %q, want %q", i, got[i], want[i])
		}
	}
}
