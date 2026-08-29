package topic

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrTopicExists   = errors.New("topic already exists")
	ErrTopicNotFound = errors.New("topic not found")
)

type Partition struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

type Topic struct {
	Name       string
	Partitions []Partition
}

type Registry struct {
	mu     sync.RWMutex
	topics map[string]Topic
}

func NewRegistry() *Registry {
	return &Registry{topics: map[string]Topic{}}
}

// cloneTopic returns a copy that shares no slice with its input, so the result
// can be read or mutated by a caller that isn't holding the registry lock.
func cloneTopic(t Topic) Topic {
	parts := make([]Partition, len(t.Partitions))
	for i, p := range t.Partitions {
		p.Replicas = slices.Clone(p.Replicas)
		p.ISR = slices.Clone(p.ISR)
		parts[i] = p
	}
	t.Partitions = parts
	return t
}

func (r *Registry) All() []Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Topic, 0, len(r.topics))
	for _, t := range r.topics {
		out = append(out, cloneTopic(t))
	}
	return out
}

func (r *Registry) Get(name string) (Topic, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.topics[name]
	if !ok {
		return Topic{}, false
	}
	return cloneTopic(t), true
}

func (r *Registry) Add(t Topic) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.topics[t.Name] = cloneTopic(t)
}

func (r *Registry) Create(t Topic) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.topics[t.Name]; ok {
		return ErrTopicExists
	}
	r.topics[t.Name] = cloneTopic(t)
	return nil
}

func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.topics[name]; !ok {
		return ErrTopicNotFound
	}
	delete(r.topics, name)
	return nil
}
