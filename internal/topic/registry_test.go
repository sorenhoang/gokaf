package topic

import (
	"strconv"
	"sync"
	"testing"
)

func sampleTopic(name string) Topic {
	return Topic{
		Name: name,
		Partitions: []Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	}
}

func TestRegistryAllReturnsAddedTopics(t *testing.T) {
	r := NewRegistry()
	r.Add(sampleTopic("orders"))
	r.Add(sampleTopic("payments"))

	if got := len(r.All()); got != 2 {
		t.Fatalf("All(): got %d topics, want 2", got)
	}
}

func TestRegistryGetMissingReturnsFalse(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Get("nope"); ok {
		t.Fatal("Get(nope): ok = true, want false")
	}
}

func TestRegistryAllResultIsIndependentOfInternalState(t *testing.T) {
	r := NewRegistry()
	r.Add(sampleTopic("orders"))

	got := r.All()
	got[0].Partitions[0].Leader = 99
	got[0].Partitions[0].Replicas[0] = 99

	fresh, _ := r.Get("orders")
	if fresh.Partitions[0].Leader != 1 || fresh.Partitions[0].Replicas[0] != 1 {
		t.Fatalf("mutating All() result leaked into registry: %+v", fresh.Partitions[0])
	}
}

func TestRegistryAddDoesNotAliasCallerSlice(t *testing.T) {
	r := NewRegistry()
	in := sampleTopic("orders")
	r.Add(in)
	in.Partitions[0].Leader = 99

	fresh, _ := r.Get("orders")
	if fresh.Partitions[0].Leader != 1 {
		t.Fatalf("mutating the slice passed to Add leaked into registry: leader=%d", fresh.Partitions[0].Leader)
	}
}

// Run with: go test -race ./internal/topic/
func TestRegistryConcurrentReadersAndWriters(t *testing.T) {
	r := NewRegistry()
	r.Add(sampleTopic("orders"))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			r.All()
			r.Get("orders")
			r.Add(sampleTopic("t" + strconv.Itoa(n)))
		}(i)
	}
	wg.Wait()
}
