package network

import (
	"slices"
	"testing"

	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestOnPeerDownPromotesLowestLiveISRWhenSelfIsCandidate(t *testing.T) {
	broker := newFailoverTestBroker(2)

	broker.OnPeerDown(1)

	orders, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("orders topic missing")
	}
	partition := orders.Partitions[0]
	if partition.Leader != 2 || !slices.Equal(partition.ISR, []int32{2, 3}) {
		t.Fatalf("partition = %+v, want leader 2 and ISR [2 3]", partition)
	}
}

func TestOnPeerDownDoesNotPromoteWhenAnotherBrokerIsLowestLiveISR(t *testing.T) {
	broker := newFailoverTestBroker(3)
	broker.ControllerID = func() int32 { return 2 }

	broker.OnPeerDown(1)

	orders, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("orders topic missing")
	}
	if got := orders.Partitions[0].Leader; got != 1 {
		t.Fatalf("leader = %d, want unchanged leader 1", got)
	}
}

func TestControllerOnPeerDownCanAssignPartitionToAnotherBroker(t *testing.T) {
	broker := newFailoverTestBroker(2)
	broker.Topics.Upsert(topic.Topic{Name: "orders", Partitions: []topic.Partition{
		{
			ID:       0,
			Leader:   3,
			Replicas: []int32{3, 1, 2},
			ISR:      []int32{3, 1, 2},
		},
	}})
	broker.ControllerID = func() int32 { return 2 }
	broker.IsPeerAlive = func(id int32) bool { return id != 3 }

	broker.OnPeerDown(3)

	got, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("topic was removed during controller failover")
	}
	if got.Partitions[0].Leader != 1 {
		t.Fatalf("leader: got %d, want 1", got.Partitions[0].Leader)
	}
	if !slices.Equal(got.Partitions[0].ISR, []int32{1, 2}) {
		t.Fatalf("ISR: got %v, want [1 2]", got.Partitions[0].ISR)
	}
}

func newFailoverTestBroker(nodeID int32) *Broker {
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{
		Name: "orders",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
		},
	})
	return &Broker{
		NodeID: nodeID,
		Topics: registry,
		IsPeerAlive: func(id int32) bool {
			return id != 1
		},
	}
}
