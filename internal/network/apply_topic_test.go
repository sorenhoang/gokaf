package network

import (
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleApplyTopicAddsTopicWithExactPartitions(t *testing.T) {
	broker := &Broker{Topics: topic.NewRegistry()}
	partitions := []topic.Partition{
		{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		{ID: 1, Leader: 2, Replicas: []int32{2}, ISR: []int32{2}},
		{ID: 2, Leader: 3, Replicas: []int32{3}, ISR: []int32{3}},
	}

	body, err := broker.handleApplyTopic(protocol.RequestHeader{APIKey: 1000}, encodeApplyTopic("orders", partitions))
	if err != nil {
		t.Fatalf("handleApplyTopic returned error: %v", err)
	}
	assertTopicResult(t, body, "orders", protocol.ErrNone)

	created, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("applied topic was not added to registry")
	}
	if !samePartitions(created.Partitions, partitions) {
		t.Fatalf("partitions = %#v, want %#v", created.Partitions, partitions)
	}
}

func TestHandleApplyTopicTreatsDuplicateAsSuccess(t *testing.T) {
	broker := &Broker{Topics: topic.NewRegistry()}
	partitions := []topic.Partition{{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}
	broker.Topics.Add(topic.Topic{Name: "orders", Partitions: partitions})

	body, err := broker.handleApplyTopic(protocol.RequestHeader{APIKey: 1000}, encodeApplyTopic("orders", partitions))
	if err != nil {
		t.Fatalf("handleApplyTopic returned error: %v", err)
	}
	assertTopicResult(t, body, "orders", protocol.ErrNone)
}

func TestHandleApplyTopicUpsertsExistingTopic(t *testing.T) {
	broker := &Broker{Topics: topic.NewRegistry()}
	broker.Topics.Add(topic.Topic{
		Name:       "orders",
		Partitions: []topic.Partition{{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}}},
	})
	partitions := []topic.Partition{{ID: 0, Leader: 2, Replicas: []int32{1, 2, 3}, ISR: []int32{2, 3}}}

	body, err := broker.handleApplyTopic(protocol.RequestHeader{APIKey: 1000}, encodeApplyTopic("orders", partitions))
	if err != nil {
		t.Fatalf("handleApplyTopic returned error: %v", err)
	}
	assertTopicResult(t, body, "orders", protocol.ErrNone)

	got, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("topic missing after apply")
	}
	if !samePartitions(got.Partitions, partitions) {
		t.Fatalf("partitions = %#v, want %#v", got.Partitions, partitions)
	}
}

func TestApplyTopicHandlerIsRegistered(t *testing.T) {
	if dispatchTable[1000] == nil {
		t.Fatal("ApplyTopic handler is not registered for internal API key 1000")
	}
}

func samePartitions(a []topic.Partition, b []topic.Partition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Leader != b[i].Leader {
			return false
		}
		if len(a[i].Replicas) != len(b[i].Replicas) || len(a[i].ISR) != len(b[i].ISR) {
			return false
		}
		for j := range a[i].Replicas {
			if a[i].Replicas[j] != b[i].Replicas[j] {
				return false
			}
		}
		for j := range a[i].ISR {
			if a[i].ISR[j] != b[i].ISR[j] {
				return false
			}
		}
	}
	return true
}
