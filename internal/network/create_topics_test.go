package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleCreateTopicsCreatesTopicWithGeneratedPartitions(t *testing.T) {
	broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}
	body, err := broker.handleCreateTopics(protocol.RequestHeader{APIKey: 19, APIVersion: 0}, createTopicsRequest("orders", 3, 1))
	if err != nil {
		t.Fatalf("handleCreateTopics: unexpected error: %v", err)
	}

	assertTopicResult(t, body, "orders", protocol.ErrNone)

	created, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("created topic was not added to registry")
	}
	if len(created.Partitions) != 3 {
		t.Fatalf("partition count: got %d, want 3", len(created.Partitions))
	}
	for i, p := range created.Partitions {
		if p.ID != int32(i) || p.Leader != 1 || len(p.Replicas) != 1 || p.Replicas[0] != 1 || len(p.ISR) != 1 || p.ISR[0] != 1 {
			t.Fatalf("partition %d: got %+v, want id=%d leader=1 replicas=[1] isr=[1]", i, p, i)
		}
	}
}

func TestHandleCreateTopicsMapsValidationErrors(t *testing.T) {
	t.Run("duplicate topic", func(t *testing.T) {
		broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}
		broker.Topics.Add(topic.Topic{Name: "orders"})

		body, err := broker.handleCreateTopics(protocol.RequestHeader{APIKey: 19, APIVersion: 0}, createTopicsRequest("orders", 3, 1))
		if err != nil {
			t.Fatalf("handleCreateTopics: unexpected error: %v", err)
		}

		assertTopicResult(t, body, "orders", protocol.ErrTopicAlreadyExists)
	})

	t.Run("invalid partitions", func(t *testing.T) {
		broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}

		body, err := broker.handleCreateTopics(protocol.RequestHeader{APIKey: 19, APIVersion: 0}, createTopicsRequest("orders", 0, 1))
		if err != nil {
			t.Fatalf("handleCreateTopics: unexpected error: %v", err)
		}

		assertTopicResult(t, body, "orders", protocol.ErrInvalidPartitions)
	})

	t.Run("invalid replication factor", func(t *testing.T) {
		broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}

		body, err := broker.handleCreateTopics(protocol.RequestHeader{APIKey: 19, APIVersion: 0}, createTopicsRequest("orders", 3, 2))
		if err != nil {
			t.Fatalf("handleCreateTopics: unexpected error: %v", err)
		}

		assertTopicResult(t, body, "orders", protocol.ErrInvalidReplicationFactor)
	})
}

func TestCreateTopicsHandlerIsRegistered(t *testing.T) {
	if dispatchTable[19] == nil {
		t.Fatal("CreateTopics handler for api_key 19 is not registered")
	}
}

func createTopicsRequest(name string, partitions int32, replicationFactor int16) []byte {
	e := protocol.NewEncoder()
	e.WriteArrayLen(1)
	e.WriteString(name)
	e.WriteInt32(partitions)
	e.WriteInt16(replicationFactor)
	e.WriteArrayLen(0)
	e.WriteArrayLen(0)
	e.WriteInt32(5000)
	return e.Bytes()
}

func assertTopicResult(t *testing.T, body []byte, wantName string, wantCode int16) {
	t.Helper()

	dec := protocol.NewDecoder(bytes.NewReader(body))
	count, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topic results: unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("topic result count: got %d, want 1", count)
	}
	name, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	code, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 topic error code: unexpected error: %v", err)
	}
	if name != wantName || code != wantCode {
		t.Fatalf("topic result: got {%q, %d}, want {%q, %d}", name, code, wantName, wantCode)
	}
}
