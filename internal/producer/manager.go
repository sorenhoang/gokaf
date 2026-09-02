package producer

import "sync"

type Decision int

const (
	Append Decision = iota
	Duplicate
	OutOfOrder
)

type Check struct {
	Decision     Decision
	CachedOffset int64
}

type Manager struct {
	mu      sync.Mutex
	nextPID int64
	seqs    map[seqKey]seqState
}

type seqKey struct {
	pid       int64
	topic     string
	partition int32
}

type seqState struct {
	lastSequence    int32
	lastBaseOffset  int64
	lastRecordCount int32
}

func NewManager() *Manager {
	return &Manager{seqs: map[seqKey]seqState{}}
}

func (m *Manager) InitProducerID() (pid int64, epoch int16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextPID++
	return m.nextPID, 0
}

func (m *Manager) CheckSequence(pid int64, topic string, partition int32, baseSeq int32, recordCount int32) Check {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkSequenceLocked(pid, topic, partition, baseSeq)
}

func (m *Manager) Committed(pid int64, topic string, partition int32, baseSeq int32, recordCount int32, baseOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.committedLocked(pid, topic, partition, baseSeq, recordCount, baseOffset)
}

func (m *Manager) AppendBatch(pid int64, topic string, partition int32, baseSeq int32, recordCount int32, appendBatch func() (int64, error)) (Check, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// ponytail: coarse lock across check + append + commit. One producer's batches
	// for one partition are serial in practice; per-key locks if this contends.
	check := m.checkSequenceLocked(pid, topic, partition, baseSeq)
	if check.Decision != Append {
		return check, nil
	}
	baseOffset, err := appendBatch()
	if err != nil {
		return check, err
	}
	m.committedLocked(pid, topic, partition, baseSeq, recordCount, baseOffset)
	return Check{Decision: Append, CachedOffset: baseOffset}, nil
}

func (m *Manager) checkSequenceLocked(pid int64, topic string, partition int32, baseSeq int32) Check {
	state, ok := m.seqs[seqKey{pid: pid, topic: topic, partition: partition}]
	if !ok {
		if baseSeq == 0 {
			return Check{Decision: Append}
		}
		return Check{Decision: OutOfOrder}
	}

	lastBaseSeq := state.lastSequence - state.lastRecordCount + 1
	if baseSeq == lastBaseSeq {
		return Check{Decision: Duplicate, CachedOffset: state.lastBaseOffset}
	}
	if baseSeq == state.lastSequence+1 {
		return Check{Decision: Append}
	}
	if baseSeq <= state.lastSequence {
		// ponytail: only the most recent batch's offset is tracked, so an older
		// duplicate reports the last batch's offset. Real Kafka keeps the last 5
		// batches per producer; enough here since a retry resends the latest.
		return Check{Decision: Duplicate, CachedOffset: state.lastBaseOffset}
	}
	return Check{Decision: OutOfOrder}
}

func (m *Manager) committedLocked(pid int64, topic string, partition int32, baseSeq int32, recordCount int32, baseOffset int64) {
	m.seqs[seqKey{pid: pid, topic: topic, partition: partition}] = seqState{
		lastSequence:    baseSeq + recordCount - 1,
		lastBaseOffset:  baseOffset,
		lastRecordCount: recordCount,
	}
}
