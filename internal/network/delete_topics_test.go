package network

import (
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleDeleteTopicsDeletesExistingTopic(t *testing.T) {
	broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}
	broker.Topics.Add(topic.Topic{Name: "orders"})

	body, err := broker.handleDeleteTopics(protocol.RequestHeader{APIKey: 20, APIVersion: 0}, deleteTopicsRequest("orders"))
	if err != nil {
		t.Fatalf("handleDeleteTopics: unexpected error: %v", err)
	}

	assertTopicResult(t, body, "orders", protocol.ErrNone)
	if _, ok := broker.Topics.Get("orders"); ok {
		t.Fatal("deleted topic is still present in registry")
	}
}

func TestHandleDeleteTopicsMapsMissingTopic(t *testing.T) {
	broker := &Broker{NodeID: 1, Topics: topic.NewRegistry()}

	body, err := broker.handleDeleteTopics(protocol.RequestHeader{APIKey: 20, APIVersion: 0}, deleteTopicsRequest("ghost"))
	if err != nil {
		t.Fatalf("handleDeleteTopics: unexpected error: %v", err)
	}

	assertTopicResult(t, body, "ghost", protocol.ErrUnknownTopicOrPartition)
}

func TestDeleteTopicsHandlerIsRegistered(t *testing.T) {
	if dispatchTable[20] == nil {
		t.Fatal("DeleteTopics handler for api_key 20 is not registered")
	}
}

func deleteTopicsRequest(name string) []byte {
	e := protocol.NewEncoder()
	e.WriteArrayLen(1)
	e.WriteString(name)
	e.WriteInt32(5000)
	return e.Bytes()
}
