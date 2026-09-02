package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleOffsetFetchReturnsCommittedOffset(t *testing.T) {
	store := newOffsetStoreForTest(t)
	if err := store.Commit("group-a", "orders", 0, 42); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	broker := &Broker{Offsets: store}

	body, err := broker.handleOffsetFetch(protocol.RequestHeader{APIKey: 9, APIVersion: 0}, offsetFetchRequest("group-a", "orders", 0))
	if err != nil {
		t.Fatalf("handleOffsetFetch: unexpected error: %v", err)
	}

	assertOffsetFetchResponse(t, body, "orders", 0, 42, nil, protocol.ErrNone)
}

func TestHandleOffsetFetchReturnsMinusOneForUncommittedOffset(t *testing.T) {
	store := newOffsetStoreForTest(t)
	broker := &Broker{Offsets: store}

	body, err := broker.handleOffsetFetch(protocol.RequestHeader{APIKey: 9, APIVersion: 0}, offsetFetchRequest("group-a", "orders", 0))
	if err != nil {
		t.Fatalf("handleOffsetFetch: unexpected error: %v", err)
	}

	assertOffsetFetchResponse(t, body, "orders", 0, -1, nil, protocol.ErrNone)
}

func TestOffsetFetchHandlerIsRegistered(t *testing.T) {
	if dispatchTable[9] == nil {
		t.Fatal("OffsetFetch handler for api_key 9 is not registered")
	}
}

func offsetFetchRequest(groupID string, topicName string, partition int32) []byte {
	e := protocol.NewEncoder()
	e.WriteString(groupID)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	return e.Bytes()
}

func assertOffsetFetchResponse(t *testing.T, body []byte, wantTopic string, wantPartition int32, wantOffset int64, wantMetadata *string, wantCode int16) {
	t.Helper()

	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topic count: unexpected error: %v", err)
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
		t.Fatalf("ReadArrayLen partition count: unexpected error: %v", err)
	}
	partition, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 partition: unexpected error: %v", err)
	}
	offset, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 offset: unexpected error: %v", err)
	}
	metadata, err := dec.ReadNullableString()
	if err != nil {
		t.Fatalf("ReadNullableString metadata: unexpected error: %v", err)
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if topicName != wantTopic || partitionCount != 1 || partition != wantPartition || offset != wantOffset || errorCode != wantCode {
		t.Fatalf("fetch response: got {%q partitions=%d partition=%d offset=%d error=%d}, want {%q partitions=1 partition=%d offset=%d error=%d}", topicName, partitionCount, partition, offset, errorCode, wantTopic, wantPartition, wantOffset, wantCode)
	}
	if metadata != wantMetadata {
		t.Fatalf("metadata: got %v, want %v", metadata, wantMetadata)
	}
}
