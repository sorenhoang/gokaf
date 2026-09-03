package cluster

import (
	"reflect"
	"testing"

	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestMetadataLogAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	ml, err := OpenMetadataLog(dir)
	if err != nil {
		t.Fatalf("OpenMetadataLog: %v", err)
	}
	records := []Record{
		{Type: TopicUpsert, Topic: "orders", Partitions: []topic.Partition{{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}}}},
		{Type: TopicDelete, Topic: "old", Partitions: nil},
	}
	for _, want := range records {
		if _, err := ml.Append(want); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := ml.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ml, err = OpenMetadataLog(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ml.Close()
	got, err := ml.ReadFrom(0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if !reflect.DeepEqual(got, records) {
		t.Fatalf("records = %#v, want %#v", got, records)
	}
	if got := ml.EndOffset(); got != 2 {
		t.Fatalf("EndOffset = %d, want 2", got)
	}
}
