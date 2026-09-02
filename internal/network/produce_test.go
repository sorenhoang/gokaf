package network

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/sorenhoang/gokaf/internal/producer"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleProduceAppendsStampedRecordBatchAndReturnsBaseOffset(t *testing.T) {
	dataDir := t.TempDir()
	broker := newProduceTestBroker(dataDir)
	requestBatch := buildRecordBatch(t, "hello")

	body, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, requestBatch))
	if err != nil {
		t.Fatalf("handleProduce: unexpected error: %v", err)
	}

	assertProduceResult(t, body, "orders", 0, protocol.ErrNone, 0)
	assertStoredBatch(t, filepath.Join(dataDir, "orders-0", "00000000000000000000.log"), requestBatch, 0)
}

func TestHandleProduceAssignsNextBaseOffset(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())

	first, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, buildRecordBatch(t, "one")))
	if err != nil {
		t.Fatalf("handleProduce first: unexpected error: %v", err)
	}
	second, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, buildRecordBatch(t, "two")))
	if err != nil {
		t.Fatalf("handleProduce second: unexpected error: %v", err)
	}

	assertProduceResult(t, first, "orders", 0, protocol.ErrNone, 0)
	assertProduceResult(t, second, "orders", 0, protocol.ErrNone, 1)
}

func TestHandleProduceDeduplicatesIdempotentRetry(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	batch := buildRecordBatch(t, "dup-me")
	stampProducerFields(batch, 1, 0, 0)

	first, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, batch))
	if err != nil {
		t.Fatalf("handleProduce first: unexpected error: %v", err)
	}
	second, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, batch))
	if err != nil {
		t.Fatalf("handleProduce retry: unexpected error: %v", err)
	}

	assertProduceResult(t, first, "orders", 0, protocol.ErrNone, 0)
	assertProduceResult(t, second, "orders", 0, protocol.ErrNone, 0)
	assertLogRecordCount(t, broker, "orders", 0, 1)
}

func TestHandleProduceRejectsOutOfOrderSequence(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	batch := buildRecordBatch(t, "gap")
	stampProducerFields(batch, 1, 0, 5)

	body, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, batch))
	if err != nil {
		t.Fatalf("handleProduce: unexpected error: %v", err)
	}

	assertProduceResult(t, body, "orders", 0, protocol.ErrOutOfOrderSequenceNumber, -1)
	assertLogRecordCount(t, broker, "orders", 0, 0)
}

func TestHandleProduceRejectsCorruptRecordBatch(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())
	batch := buildRecordBatch(t, "hello")
	batch[len(batch)-1] ^= 0xff

	body, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 0, batch))
	if err != nil {
		t.Fatalf("handleProduce: unexpected error: %v", err)
	}

	assertProduceResult(t, body, "orders", 0, protocol.ErrCorruptMessage, -1)
}

func TestHandleProduceRejectsUnknownTopicOrPartition(t *testing.T) {
	broker := newProduceTestBroker(t.TempDir())

	unknownTopic, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("ghost", 0, buildRecordBatch(t, "hello")))
	if err != nil {
		t.Fatalf("handleProduce unknown topic: unexpected error: %v", err)
	}
	unknownPartition, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest("orders", 1, buildRecordBatch(t, "hello")))
	if err != nil {
		t.Fatalf("handleProduce unknown partition: unexpected error: %v", err)
	}

	assertProduceResult(t, unknownTopic, "ghost", 0, protocol.ErrUnknownTopicOrPartition, -1)
	assertProduceResult(t, unknownPartition, "orders", 1, protocol.ErrUnknownTopicOrPartition, -1)
}

func TestProduceHandlerIsRegistered(t *testing.T) {
	if dispatchTable[0] == nil {
		t.Fatal("Produce handler for api_key 0 is not registered")
	}
}

func newProduceTestBroker(dataDir string) *Broker {
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{
		Name: "orders",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})

	return &Broker{
		NodeID:    1,
		Host:      "localhost",
		Port:      9092,
		Topics:    registry,
		Logs:      storage.NewManager(dataDir),
		Producers: producer.NewManager(),
	}
}

func produceRequest(topicName string, partition int32, batch []byte) []byte {
	e := protocol.NewEncoder()
	e.WriteInt16(1)
	e.WriteInt32(5000)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt32(int32(len(batch)))
	return append(e.Bytes(), batch...)
}

func assertProduceResult(t *testing.T, body []byte, wantTopic string, wantPartition int32, wantCode int16, wantBaseOffset int64) {
	t.Helper()

	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topic responses: unexpected error: %v", err)
	}
	if topicCount != 1 {
		t.Fatalf("topic response count: got %d, want 1", topicCount)
	}
	topicName, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen partition responses: unexpected error: %v", err)
	}
	if topicName != wantTopic || partitionCount != 1 {
		t.Fatalf("topic response: got {%q, partitions=%d}, want {%q, partitions=1}", topicName, partitionCount, wantTopic)
	}
	partition, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 partition: unexpected error: %v", err)
	}
	code, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	baseOffset, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 base offset: unexpected error: %v", err)
	}
	if partition != wantPartition || code != wantCode || baseOffset != wantBaseOffset {
		t.Fatalf("partition response: got {partition=%d, code=%d, base_offset=%d}, want {%d, %d, %d}",
			partition, code, baseOffset, wantPartition, wantCode, wantBaseOffset)
	}
}

func assertStoredBatch(t *testing.T, path string, requestBatch []byte, wantBaseOffset int64) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile segment: unexpected error: %v", err)
	}
	if len(data) != 12+len(requestBatch) {
		t.Fatalf("segment length: got %d, want %d", len(data), 12+len(requestBatch))
	}
	storageOffset := int64(binary.BigEndian.Uint64(data[0:8]))
	payloadLen := int(binary.BigEndian.Uint32(data[8:12]))
	if storageOffset != wantBaseOffset || payloadLen != len(requestBatch) {
		t.Fatalf("storage entry header: got {offset=%d, length=%d}, want {%d, %d}", storageOffset, payloadLen, wantBaseOffset, len(requestBatch))
	}

	storedBatch := data[12:]
	if int64(binary.BigEndian.Uint64(storedBatch[0:8])) != wantBaseOffset {
		t.Fatalf("batch baseOffset: got %d, want %d", int64(binary.BigEndian.Uint64(storedBatch[0:8])), wantBaseOffset)
	}
	if int32(binary.BigEndian.Uint32(storedBatch[8:12])) != int32(len(storedBatch)-12) {
		t.Fatalf("batchLength: got %d, want %d", int32(binary.BigEndian.Uint32(storedBatch[8:12])), len(storedBatch)-12)
	}
	if storedBatch[16] != 2 {
		t.Fatalf("magic: got %d, want 2", storedBatch[16])
	}
	wantCRC := crc32.Checksum(storedBatch[21:], crc32.MakeTable(crc32.Castagnoli))
	gotCRC := binary.BigEndian.Uint32(storedBatch[17:21])
	if gotCRC != wantCRC {
		t.Fatalf("crc: got %d, want %d", gotCRC, wantCRC)
	}
}

func assertLogRecordCount(t *testing.T, broker *Broker, topic string, partition int32, want int) {
	t.Helper()

	log, err := broker.Logs.Log(topic, partition)
	if err != nil {
		t.Fatalf("Log: unexpected error: %v", err)
	}
	records, err := log.Read(0, -1)
	if err != nil {
		t.Fatalf("Read: unexpected error: %v", err)
	}
	if len(records) != want {
		t.Fatalf("record count: got %d, want %d", len(records), want)
	}
}

func stampProducerFields(batch []byte, pid int64, epoch int16, baseSequence int32) {
	putInt64(batch[43:51], pid)
	putInt16(batch[51:53], epoch)
	putInt32(batch[53:57], baseSequence)
	crc := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], crc)
}

func buildRecordBatch(t *testing.T, values ...string) []byte {
	t.Helper()

	var records []byte
	for i, value := range values {
		records = append(records, buildRecord(value, int32(i))...)
	}

	batch := make([]byte, 61, 61+len(records))
	binary.BigEndian.PutUint64(batch[0:8], 0)
	putInt32(batch[12:16], -1)
	batch[16] = 2
	binary.BigEndian.PutUint16(batch[21:23], 0)
	putInt32(batch[23:27], int32(len(values)-1))
	binary.BigEndian.PutUint64(batch[27:35], uint64(int64(1700000000000)))
	binary.BigEndian.PutUint64(batch[35:43], uint64(int64(1700000000000)))
	putInt64(batch[43:51], -1)
	putInt16(batch[51:53], -1)
	putInt32(batch[53:57], -1)
	putInt32(batch[57:61], int32(len(values)))
	batch = append(batch, records...)

	putInt32(batch[8:12], int32(len(batch)-12))
	crc := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], crc)
	return batch
}

func putInt16(dst []byte, value int16) {
	binary.BigEndian.PutUint16(dst, uint16(value))
}

func putInt32(dst []byte, value int32) {
	binary.BigEndian.PutUint32(dst, uint32(value))
}

func putInt64(dst []byte, value int64) {
	binary.BigEndian.PutUint64(dst, uint64(value))
}

func buildRecord(value string, offsetDelta int32) []byte {
	body := protocol.NewEncoder()
	body.WriteInt8(0)
	body.WriteVarint(0)
	body.WriteVarint(offsetDelta)
	body.WriteVarint(-1)
	body.WriteVarint(int32(len(value)))
	recordBody := append(body.Bytes(), []byte(value)...)
	trailer := protocol.NewEncoder()
	trailer.WriteVarint(0)
	recordBody = append(recordBody, trailer.Bytes()...)

	record := protocol.NewEncoder()
	record.WriteVarint(int32(len(recordBody)))
	return append(record.Bytes(), recordBody...)
}
