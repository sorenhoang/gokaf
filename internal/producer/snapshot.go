package producer

import "sort"

// ProducerSnapshot is a read-only view of one active producer id.
type ProducerSnapshot struct {
	ProducerID int64
	Epoch      int16
	Partitions []ProducerPartition
}

// ProducerPartition is the dedup state for one (topic, partition) the producer
// has written to.
type ProducerPartition struct {
	Topic        string
	Partition    int32
	LastSequence int32
	LastOffset   int64
}

// Snapshot returns every producer id that has written at least one batch, with
// its per-partition sequence state. Ordered by producer id then topic/partition.
func (m *Manager) Snapshot() []ProducerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	byPID := map[int64][]ProducerPartition{}
	for key, state := range m.seqs {
		byPID[key.pid] = append(byPID[key.pid], ProducerPartition{
			Topic:        key.topic,
			Partition:    key.partition,
			LastSequence: state.lastSequence,
			LastOffset:   state.lastBaseOffset,
		})
	}

	out := make([]ProducerSnapshot, 0, len(byPID))
	for pid, parts := range byPID {
		sort.Slice(parts, func(i, j int) bool {
			if parts[i].Topic != parts[j].Topic {
				return parts[i].Topic < parts[j].Topic
			}
			return parts[i].Partition < parts[j].Partition
		})
		out = append(out, ProducerSnapshot{ProducerID: pid, Epoch: 0, Partitions: parts})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProducerID < out[j].ProducerID })
	return out
}
