package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleFetchMetadataLogReturnsRecordsFromOffset(t *testing.T) {
	ml, err := cluster.OpenMetadataLog(t.TempDir())
	if err != nil {
		t.Fatalf("OpenMetadataLog: %v", err)
	}
	defer ml.Close()
	if _, err := ml.Append(cluster.Record{Type: cluster.TopicUpsert, Topic: "orders", Partitions: []topic.Partition{{ID: 0, Leader: 1}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	request := protocol.NewEncoder()
	request.WriteInt64(0)
	body, err := (&Broker{MetadataLog: ml}).handleFetchMetadataLog(protocol.RequestHeader{APIKey: cluster.InternalFetchMetadataLogKey}, request.Bytes())
	if err != nil {
		t.Fatalf("handleFetchMetadataLog: %v", err)
	}
	dec := protocol.NewDecoder(bytes.NewReader(body))
	count, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen: %v", err)
	}
	if count != 1 {
		t.Fatalf("record count = %d, want 1", count)
	}
	if _, err := dec.ReadBytes(); err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
}

func TestFetchMetadataLogHandlerIsRegistered(t *testing.T) {
	if dispatchTable[cluster.InternalFetchMetadataLogKey] == nil {
		t.Fatal("FetchMetadataLog handler is not registered")
	}
}
