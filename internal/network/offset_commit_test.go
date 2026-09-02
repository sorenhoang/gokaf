package network

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sorenhoang/gokaf/internal/offset"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
)

func TestHandleOffsetCommitStoresOffsetAndWritesResponse(t *testing.T) {
	store := newOffsetStoreForTest(t)
	broker := &Broker{Offsets: store}

	body, err := broker.handleOffsetCommit(protocol.RequestHeader{APIKey: 8, APIVersion: 0}, offsetCommitRequest("group-a", "orders", 0, 42))
	if err != nil {
		t.Fatalf("handleOffsetCommit: unexpected error: %v", err)
	}

	assertOffsetCommitResponse(t, body, "orders", 0, protocol.ErrNone)
	if got := store.Fetch("group-a", "orders", 0); got != 42 {
		t.Fatalf("stored offset: got %d, want 42", got)
	}
}

func TestOffsetCommitHandlerIsRegistered(t *testing.T) {
	if dispatchTable[8] == nil {
		t.Fatal("OffsetCommit handler for api_key 8 is not registered")
	}
}

func newOffsetStoreForTest(t *testing.T) *offset.Store {
	t.Helper()

	log, err := storage.Open(filepath.Join(t.TempDir(), "__consumer_offsets-0"))
	if err != nil {
		t.Fatalf("open log: unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("close log: unexpected error: %v", err)
		}
	})
	store, err := offset.NewStore(log)
	if err != nil {
		t.Fatalf("NewStore: unexpected error: %v", err)
	}
	return store
}

func offsetCommitRequest(groupID string, topicName string, partition int32, committedOffset int64) []byte {
	e := protocol.NewEncoder()
	e.WriteString(groupID)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(committedOffset)
	e.WriteNullableString(nil)
	return e.Bytes()
}

func assertOffsetCommitResponse(t *testing.T, body []byte, wantTopic string, wantPartition int32, wantCode int16) {
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
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if topicName != wantTopic || partitionCount != 1 || partition != wantPartition || errorCode != wantCode {
		t.Fatalf("commit response: got {%q partitions=%d partition=%d error=%d}, want {%q partitions=1 partition=%d error=%d}", topicName, partitionCount, partition, errorCode, wantTopic, wantPartition, wantCode)
	}
}
