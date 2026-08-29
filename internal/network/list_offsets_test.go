package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleListOffsetsReturnsLatestAndEarliestPerPartition(t *testing.T) {
	broker := newMultiPartitionTestBroker(t.TempDir())
	produceTestMessagesToPartition(t, broker, "events", 0, "a", "b", "c", "d", "e")
	produceTestMessagesToPartition(t, broker, "events", 1, "f", "g")

	latestP0, err := broker.handleListOffsets(protocol.RequestHeader{APIKey: 2, APIVersion: 1}, listOffsetsRequest("events", 0, -1))
	if err != nil {
		t.Fatalf("handleListOffsets latest p0: unexpected error: %v", err)
	}
	latestP1, err := broker.handleListOffsets(protocol.RequestHeader{APIKey: 2, APIVersion: 1}, listOffsetsRequest("events", 1, -1))
	if err != nil {
		t.Fatalf("handleListOffsets latest p1: unexpected error: %v", err)
	}
	earliestP0, err := broker.handleListOffsets(protocol.RequestHeader{APIKey: 2, APIVersion: 1}, listOffsetsRequest("events", 0, -2))
	if err != nil {
		t.Fatalf("handleListOffsets earliest p0: unexpected error: %v", err)
	}

	assertListOffsetResult(t, latestP0, "events", 0, protocol.ErrNone, -1, 5)
	assertListOffsetResult(t, latestP1, "events", 1, protocol.ErrNone, -1, 2)
	assertListOffsetResult(t, earliestP0, "events", 0, protocol.ErrNone, -1, 0)
}

func TestHandleListOffsetsRejectsUnknownTopicOrPartition(t *testing.T) {
	broker := newMultiPartitionTestBroker(t.TempDir())

	unknownTopic, err := broker.handleListOffsets(protocol.RequestHeader{APIKey: 2, APIVersion: 1}, listOffsetsRequest("ghost", 0, -1))
	if err != nil {
		t.Fatalf("handleListOffsets unknown topic: unexpected error: %v", err)
	}
	unknownPartition, err := broker.handleListOffsets(protocol.RequestHeader{APIKey: 2, APIVersion: 1}, listOffsetsRequest("events", 3, -1))
	if err != nil {
		t.Fatalf("handleListOffsets unknown partition: unexpected error: %v", err)
	}

	assertListOffsetResult(t, unknownTopic, "ghost", 0, protocol.ErrUnknownTopicOrPartition, -1, -1)
	assertListOffsetResult(t, unknownPartition, "events", 3, protocol.ErrUnknownTopicOrPartition, -1, -1)
}

func TestMultiPartitionProduceFetchIsolation(t *testing.T) {
	broker := newMultiPartitionTestBroker(t.TempDir())
	produceTestMessagesToPartition(t, broker, "events", 0, "a", "b")
	produceTestMessagesToPartition(t, broker, "events", 1, "c")
	produceTestMessagesToPartition(t, broker, "events", 2, "d", "e", "f")

	fetchP2, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("events", 2, 0, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch p2: unexpected error: %v", err)
	}
	values, highWatermark := assertFetchResult(t, fetchP2, "events", 2, protocol.ErrNone)
	if highWatermark != 3 {
		t.Fatalf("p2 high watermark: got %d, want 3", highWatermark)
	}
	assertValues(t, values, []string{"d", "e", "f"})

	fetchP1, err := broker.handleFetch(protocol.RequestHeader{APIKey: 1, APIVersion: 0}, fetchRequest("events", 1, 0, 1<<20))
	if err != nil {
		t.Fatalf("handleFetch p1: unexpected error: %v", err)
	}
	values, highWatermark = assertFetchResult(t, fetchP1, "events", 1, protocol.ErrNone)
	if highWatermark != 1 {
		t.Fatalf("p1 high watermark: got %d, want 1", highWatermark)
	}
	assertValues(t, values, []string{"c"})
}

func TestListOffsetsHandlerIsRegistered(t *testing.T) {
	if dispatchTable[2] == nil {
		t.Fatal("ListOffsets handler for api_key 2 is not registered")
	}
}

func newMultiPartitionTestBroker(dataDir string) *Broker {
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{
		Name: "events",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			{ID: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			{ID: 2, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})

	return &Broker{
		NodeID: 1,
		Host:   "localhost",
		Port:   9092,
		Topics: registry,
		Logs:   storage.NewManager(dataDir),
	}
}

func produceTestMessagesToPartition(t *testing.T, broker *Broker, topicName string, partition int32, values ...string) {
	t.Helper()

	for _, value := range values {
		if _, err := broker.handleProduce(protocol.RequestHeader{APIKey: 0, APIVersion: 0}, produceRequest(topicName, partition, buildRecordBatch(t, value))); err != nil {
			t.Fatalf("handleProduce %s-%d: unexpected error: %v", topicName, partition, err)
		}
	}
}

func listOffsetsRequest(topicName string, partition int32, timestamp int64) []byte {
	e := protocol.NewEncoder()
	e.WriteInt32(-1)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(timestamp)
	return e.Bytes()
}

func assertListOffsetResult(t *testing.T, body []byte, wantTopic string, wantPartition int32, wantCode int16, wantTimestamp int64, wantOffset int64) {
	t.Helper()

	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topics: unexpected error: %v", err)
	}
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}
	topicName, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen partitions: unexpected error: %v", err)
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
	timestamp, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 timestamp: unexpected error: %v", err)
	}
	offset, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 offset: unexpected error: %v", err)
	}
	if partition != wantPartition || code != wantCode || timestamp != wantTimestamp || offset != wantOffset {
		t.Fatalf("partition response: got {partition=%d, code=%d, timestamp=%d, offset=%d}, want {%d, %d, %d, %d}",
			partition, code, timestamp, offset, wantPartition, wantCode, wantTimestamp, wantOffset)
	}
}
