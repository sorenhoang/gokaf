package topic

import (
	"errors"
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
