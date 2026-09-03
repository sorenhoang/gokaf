package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestTopicsEndpointReturnsPartitionOffsets(t *testing.T) {
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{Name: "orders", Partitions: []topic.Partition{{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}})
	broker := &network.Broker{NodeID: 1, Host: "localhost", Port: 9092, Topics: registry}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/api/v1/topics", nil)
	New(broker).ServeHTTP(recorder, request)
	var got []network.TopicInfo
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "orders" || len(got[0].Partitions) != 1 {
		t.Fatalf("unexpected topics response: %#v", got)
	}
	partition := got[0].Partitions[0]
	if partition.ID != 0 || partition.Leader != 1 || partition.StartOffset != 0 || partition.EndOffset != 0 || partition.HighWatermark != 0 {
		t.Fatalf("unexpected partition response: %#v", partition)
	}
}

func TestBrokerEndpointReturnsBrokerInfo(t *testing.T) {
	broker := &network.Broker{NodeID: 2, Host: "localhost", Port: 9093, ControllerID: func() int32 { return 3 }}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/api/v1/broker", nil)
	New(broker).ServeHTTP(recorder, request)
	var got network.BrokerInfo
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	want := network.BrokerInfo{NodeID: 2, Host: "localhost", Port: 9093, ControllerID: 3}
	if got != want {
		t.Fatalf("broker info: got %#v, want %#v", got, want)
	}
}
