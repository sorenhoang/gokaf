package topic

import (
	"errors"
	"slices"
	"testing"
)

func TestRegistryCreateRejectsDuplicateTopic(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Create(Topic{Name: "orders"}); err != nil {
		t.Fatalf("Create first topic: unexpected error: %v", err)
	}

	err := registry.Create(Topic{Name: "orders"})
	if !errors.Is(err, ErrTopicExists) {
		t.Fatalf("Create duplicate: got %v, want ErrTopicExists", err)
	}
}

func TestRegistryUpsertReplacesExistingTopic(t *testing.T) {
	registry := NewRegistry()
	registry.Add(Topic{
		Name: "orders",
		Partitions: []Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
		},
	})

	registry.Upsert(Topic{
		Name: "orders",
		Partitions: []Partition{
			{ID: 0, Leader: 2, Replicas: []int32{1, 2, 3}, ISR: []int32{2, 3}},
		},
	})

	got, ok := registry.Get("orders")
	if !ok {
		t.Fatal("topic missing after upsert")
	}
	if got.Partitions[0].Leader != 2 || !slices.Equal(got.Partitions[0].ISR, []int32{2, 3}) {
		t.Fatalf("upserted partition = %+v, want leader 2 and ISR [2 3]", got.Partitions[0])
	}
}

func TestRegistryDeleteRemovesTopicAndRejectsMissingTopic(t *testing.T) {
	registry := NewRegistry()
	registry.Add(Topic{Name: "orders"})

	if err := registry.Delete("orders"); err != nil {
		t.Fatalf("Delete existing topic: unexpected error: %v", err)
	}
	if _, ok := registry.Get("orders"); ok {
		t.Fatal("Delete existing topic: topic is still present")
	}

	err := registry.Delete("orders")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Delete missing topic: got %v, want ErrTopicNotFound", err)
	}
}
