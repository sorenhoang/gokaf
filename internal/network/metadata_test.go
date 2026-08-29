package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleMetadataAllTopicsWritesBrokerTopicsAndPartitionLeaders(t *testing.T) {
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{
		Name: "orders",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			{ID: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})

	broker := &Broker{NodeID: 1, Host: "localhost", Port: 9092, Topics: registry}
	req := protocol.NewEncoder()
	req.WriteArrayLen(0)

	body, err := broker.handleMetadata(protocol.RequestHeader{APIKey: 3, APIVersion: 0}, req.Bytes())
	if err != nil {
		t.Fatalf("handleMetadata: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	brokerCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen brokers: unexpected error: %v", err)
	}
	if brokerCount != 1 {
		t.Fatalf("broker count: got %d, want 1", brokerCount)
	}

	nodeID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 broker node id: unexpected error: %v", err)
	}
	host, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString broker host: unexpected error: %v", err)
	}
	port, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 broker port: unexpected error: %v", err)
	}
	if nodeID != 1 || host != "localhost" || port != 9092 {
		t.Fatalf("broker: got {%d, %q, %d}, want {1, %q, 9092}", nodeID, host, port, "localhost")
	}

	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topics: unexpected error: %v", err)
	}
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}

	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 topic error code: unexpected error: %v", err)
	}
	name, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen partitions: unexpected error: %v", err)
	}
	if errorCode != 0 || name != "orders" || partitionCount != 2 {
		t.Fatalf("topic: got {error=%d, name=%q, partitions=%d}, want {0, %q, 2}", errorCode, name, partitionCount, "orders")
	}

	partitionErrorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 partition error code: unexpected error: %v", err)
	}
	partitionIndex, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 partition index: unexpected error: %v", err)
	}
	leaderID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 leader id: unexpected error: %v", err)
	}
	replicaCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen replicas: unexpected error: %v", err)
	}
	replicaID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 replica id: unexpected error: %v", err)
	}
	isrCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen isr: unexpected error: %v", err)
	}
	isrID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 isr id: unexpected error: %v", err)
	}

	if partitionErrorCode != 0 || partitionIndex != 0 || leaderID != 1 || replicaCount != 1 || replicaID != 1 || isrCount != 1 || isrID != 1 {
		t.Fatalf("partition: got {error=%d, index=%d, leader=%d, replicas=[%d/%d], isr=[%d/%d]}, want {0, 0, 1, replicas=[1/1], isr=[1/1]}",
			partitionErrorCode, partitionIndex, leaderID, replicaID, replicaCount, isrID, isrCount)
	}
}

func TestHandleMetadataUnknownTopicReturnsErrorCode3(t *testing.T) {
	broker := &Broker{NodeID: 1, Host: "localhost", Port: 9092, Topics: topic.NewRegistry()}

	req := protocol.NewEncoder()
	req.WriteArrayLen(1)
	req.WriteString("ghost")

	body, err := broker.handleMetadata(protocol.RequestHeader{APIKey: 3, APIVersion: 0}, req.Bytes())
	if err != nil {
		t.Fatalf("handleMetadata: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	skipMetadataBrokers(t, dec)

	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen topics: unexpected error: %v", err)
	}
	if topicCount != 1 {
		t.Fatalf("topic count: got %d, want 1", topicCount)
	}

	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 topic error code: unexpected error: %v", err)
	}
	name, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString topic name: unexpected error: %v", err)
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen partitions: unexpected error: %v", err)
	}
	if errorCode != protocol.ErrUnknownTopicOrPartition || name != "ghost" || partitionCount != 0 {
		t.Fatalf("unknown topic: got {error=%d, name=%q, partitions=%d}, want {3, %q, 0}", errorCode, name, partitionCount, "ghost")
	}
}

func skipMetadataBrokers(t *testing.T, dec *protocol.Decoder) {
	t.Helper()

	brokerCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen brokers: unexpected error: %v", err)
	}
	for i := 0; i < brokerCount; i++ {
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 broker node id: unexpected error: %v", err)
		}
		if _, err := dec.ReadString(); err != nil {
			t.Fatalf("ReadString broker host: unexpected error: %v", err)
		}
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("ReadInt32 broker port: unexpected error: %v", err)
		}
	}
}

func TestMetadataHandlerIsRegistered(t *testing.T) {
	if dispatchTable[3] == nil {
		t.Fatal("Metadata handler for api_key 3 is not registered")
	}
}
