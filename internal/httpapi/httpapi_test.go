package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func newTestBroker(t *testing.T) *network.Broker {
	t.Helper()
	registry := topic.NewRegistry()
	registry.Add(topic.Topic{Name: "orders", Partitions: []topic.Partition{{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}})
	logs := storage.NewManager(filepath.Join(t.TempDir(), "data"))
	t.Cleanup(func() { _ = logs.Close() })
	return &network.Broker{NodeID: 1, Host: "localhost", Port: 9092, Topics: registry, Logs: logs}
}

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, reader))
	return rec
}

func TestTopicsEndpointReturnsPartitionOffsets(t *testing.T) {
	rec := do(t, New(newTestBroker(t)), "GET", "/api/v1/topics", nil)
	var got []network.TopicInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "orders" || len(got[0].Partitions) != 1 {
		t.Fatalf("unexpected topics response: %#v", got)
	}
	p := got[0].Partitions[0]
	if p.ID != 0 || p.Leader != 1 || p.StartOffset != 0 || p.EndOffset != 0 || p.HighWatermark != 0 {
		t.Fatalf("unexpected partition response: %#v", p)
	}
}

func TestBrokerEndpointReturnsBrokerInfo(t *testing.T) {
	broker := &network.Broker{NodeID: 2, Host: "localhost", Port: 9093, ControllerID: func() int32 { return 3 }}
	rec := do(t, New(broker), "GET", "/api/v1/broker", nil)
	var got network.BrokerInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	want := network.BrokerInfo{NodeID: 2, Host: "localhost", Port: 9093, ControllerID: 3}
	if got != want {
		t.Fatalf("broker info: got %#v, want %#v", got, want)
	}
}

func TestProduceThenFetchRoundTrip(t *testing.T) {
	h := New(newTestBroker(t))

	rec := do(t, h, "POST", "/api/v1/produce", map[string]any{"topic": "orders", "partition": 0, "key": "k1", "value": "hello"})
	if rec.Code != http.StatusOK {
		t.Fatalf("produce status = %d, body %s", rec.Code, rec.Body)
	}
	var produced struct {
		BaseOffset int64 `json:"base_offset"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&produced); err != nil {
		t.Fatal(err)
	}
	if produced.BaseOffset != 0 {
		t.Fatalf("base_offset = %d, want 0", produced.BaseOffset)
	}

	rec = do(t, h, "POST", "/api/v1/fetch", map[string]any{"topic": "orders", "partition": 0, "offset": 0})
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch status = %d, body %s", rec.Code, rec.Body)
	}
	var fetched struct {
		HighWatermark int64 `json:"high_watermark"`
		Records       []struct {
			Offset int64  `json:"offset"`
			Key    string `json:"key"`
			Value  string `json:"value"`
		} `json:"records"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.HighWatermark != 1 || len(fetched.Records) != 1 {
		t.Fatalf("fetch response = %+v", fetched)
	}
	if fetched.Records[0].Offset != 0 || fetched.Records[0].Key != "k1" || fetched.Records[0].Value != "hello" {
		t.Fatalf("fetched record = %+v", fetched.Records[0])
	}
}

func TestProduceToMissingTopicReturns404(t *testing.T) {
	rec := do(t, New(newTestBroker(t)), "POST", "/api/v1/produce", map[string]any{"topic": "ghost", "partition": 0, "value": "x"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestCreateAndDeleteTopic(t *testing.T) {
	h := New(newTestBroker(t))

	rec := do(t, h, "POST", "/api/v1/topics", map[string]any{"name": "events", "partitions": 2})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body)
	}

	rec = do(t, h, "GET", "/api/v1/topics", nil)
	var topics []network.TopicInfo
	_ = json.NewDecoder(rec.Body).Decode(&topics)
	if !hasTopic(topics, "events") {
		t.Fatalf("events not present after create: %#v", topics)
	}

	rec = do(t, h, "DELETE", "/api/v1/topics/events", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", rec.Code)
	}
	rec = do(t, h, "GET", "/api/v1/topics", nil)
	topics = nil
	_ = json.NewDecoder(rec.Body).Decode(&topics)
	if hasTopic(topics, "events") {
		t.Fatalf("events still present after delete: %#v", topics)
	}
}

func hasTopic(topics []network.TopicInfo, name string) bool {
	for _, t := range topics {
		if t.Name == name {
			return true
		}
	}
	return false
}

func TestGroupsAndProducersEndpointsReturnJSON(t *testing.T) {
	h := New(newTestBroker(t))

	rec := do(t, h, "GET", "/api/v1/groups", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("groups status = %d", rec.Code)
	}
	var groups []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&groups); err != nil {
		t.Fatalf("groups body: %v", err)
	}

	rec = do(t, h, "GET", "/api/v1/producers", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("producers status = %d", rec.Code)
	}
	var producers []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&producers); err != nil {
		t.Fatalf("producers body: %v", err)
	}
}

func TestResetGroupOffsetWithoutStoreIs500(t *testing.T) {
	h := New(newTestBroker(t)) // no Offsets store wired
	rec := do(t, h, "POST", "/api/v1/groups/g1/reset-offset", map[string]any{"topic": "orders", "partition": 0, "offset": 5})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}
