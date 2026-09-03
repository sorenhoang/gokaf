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
	lagTimeout time.Duration
	mu         sync.Mutex
	fetchers   map[tp]context.CancelFunc
	led        map[tp]*PartitionState
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
		lagTimeout: 10 * time.Second,
		fetchers:   map[tp]context.CancelFunc{},
		led:        map[tp]*PartitionState{},
	}
}

func (m *Manager) StartFollowing(t topic.Topic) {
	for _, partition := range t.Partitions {
		if len(partition.Replicas) == 0 || !containsBroker(partition.Replicas, m.selfID) {
			continue
		}
		key := tp{topic: t.Name, partition: partition.ID}
		if partition.Leader == m.selfID {
			m.stopFollowingKey(key)
			m.mu.Lock()
			if _, ok := m.led[key]; !ok {
				replicas := partition.ISR
				if len(replicas) == 0 {
					replicas = partition.Replicas
				}
				m.led[key] = NewPartitionState(replicas, m.selfID, m.lagTimeout)
			}
			m.mu.Unlock()
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

		m.mu.Lock()
		if _, ok := m.fetchers[key]; ok {
			m.mu.Unlock()
			continue
		}
		delete(m.led, key)
		ctx, cancel := context.WithCancel(context.Background())
		m.fetchers[key] = cancel
		m.mu.Unlock()

		f := fetcher{
			topic:     t.Name,
			partition: partition.ID,
			selfID:    m.selfID,
			leader:    leader,
			localLog:  localLog,
			interval:  m.interval,
			maxBytes:  1 << 20,
		}
		go f.run(ctx)
	}
}

func (m *Manager) StopFollowing(topic string, partition int32) {
	m.stopFollowingKey(tp{topic: topic, partition: partition})
}

func (m *Manager) Lead(topic string, partition int32, isr []int32) {
	key := tp{topic: topic, partition: partition}
	m.stopFollowingKey(key)
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.led[key]; !ok {
		m.led[key] = NewPartitionState(isr, m.selfID, m.lagTimeout)
	}
}

func (m *Manager) RecordFollowerFetch(topic string, partition, brokerID int32, fetchOffset, leaderEndOffset int64) {
	state := m.partitionState(topic, partition)
	if state == nil {
		return
	}
	state.RecordFollowerFetch(brokerID, fetchOffset, leaderEndOffset)
}

func (m *Manager) WaitForHighWatermark(topic string, partition int32, target int64, timeout time.Duration) error {
	state := m.partitionState(topic, partition)
	if state == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return state.WaitForHighWatermark(target, timeout)
}

func (m *Manager) HighWatermark(topic string, partition int32, leaderEndOffset int64) int64 {
	state := m.partitionState(topic, partition)
	if state == nil {
		return leaderEndOffset
	}
	return state.HighWatermark(leaderEndOffset)
}

func (m *Manager) ISR(topic string, partition int32) []int32 {
	state := m.partitionState(topic, partition)
	if state == nil {
		return nil
	}
	return state.ISR()
}

func (m *Manager) partitionState(topic string, partition int32) *PartitionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.led[tp{topic: topic, partition: partition}]
}

func (m *Manager) stopFollowingKey(key tp) {
	m.mu.Lock()
	cancel, ok := m.fetchers[key]
	if ok {
		delete(m.fetchers, key)
	}
	m.mu.Unlock()
	if ok {
		cancel()
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
