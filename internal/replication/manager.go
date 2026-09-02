package replication

import (
	"context"
	"sync"
	"time"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

type Manager struct {
	selfID     int32
	logs       *storage.Manager
	membership *cluster.Membership
	interval   time.Duration
	mu         sync.Mutex
	fetchers   map[tp]context.CancelFunc
}

type tp struct {
	topic     string
	partition int32
}

func NewManager(selfID int32, logs *storage.Manager, membership *cluster.Membership, interval time.Duration) *Manager {
	return &Manager{
		selfID:     selfID,
		logs:       logs,
		membership: membership,
		interval:   interval,
		fetchers:   map[tp]context.CancelFunc{},
	}
}

func (m *Manager) StartFollowing(t topic.Topic) {
	for _, partition := range t.Partitions {
		if len(partition.Replicas) == 0 || partition.Replicas[0] == m.selfID || !containsBroker(partition.Replicas, m.selfID) {
			continue
		}
		leader, ok := m.membership.Get(partition.Leader)
		if !ok {
			continue
		}
		localLog, err := m.logs.Log(t.Name, partition.ID)
		if err != nil {
			continue
		}

		key := tp{topic: t.Name, partition: partition.ID}
		m.mu.Lock()
		if _, ok := m.fetchers[key]; ok {
			m.mu.Unlock()
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		m.fetchers[key] = cancel
		m.mu.Unlock()

		f := fetcher{
			topic:     t.Name,
			partition: partition.ID,
			leader:    leader,
			localLog:  localLog,
			interval:  m.interval,
			maxBytes:  1 << 20,
		}
		go f.run(ctx)
	}
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, cancel := range m.fetchers {
		cancel()
		delete(m.fetchers, key)
	}
}

func containsBroker(ids []int32, want int32) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
