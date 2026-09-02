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

	broker.OnPeerDown(1)

	orders, ok := broker.Topics.Get("orders")
	if !ok {
		t.Fatal("orders topic missing")
	}
	if got := orders.Partitions[0].Leader; got != 1 {
		t.Fatalf("leader = %d, want unchanged leader 1", got)
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
