package replication

import (
	"errors"
	"sync"
	"time"
)

var errHighWatermarkTimedOut = errors.New("replication: high watermark wait timed out")

type PartitionState struct {
	mu              sync.Mutex
	replicas        []int32
	leaderID        int32
	followerOffset  map[int32]int64
	followerSeen    map[int32]time.Time
	leaderEndOffset int64
	highWatermark   int64
	lagTimeout      time.Duration
	waiters         []*hwWaiter
}

type hwWaiter struct {
	target int64
	done   chan struct{}
}

func NewPartitionState(replicas []int32, leaderID int32, lagTimeout time.Duration) *PartitionState {
	now := time.Now()
	copied := append([]int32(nil), replicas...)
	state := &PartitionState{
		replicas:       copied,
		leaderID:       leaderID,
		followerOffset: map[int32]int64{},
		followerSeen:   map[int32]time.Time{},
		lagTimeout:     lagTimeout,
	}
	for _, brokerID := range copied {
		if brokerID == leaderID {
			continue
		}
		state.followerOffset[brokerID] = 0
		state.followerSeen[brokerID] = now
	}
	state.recompute(now)
	return state
}

func (s *PartitionState) RecordFollowerFetch(brokerID int32, fetchOffset, leaderEndOffset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.followerOffset[brokerID] = fetchOffset
	s.followerSeen[brokerID] = time.Now()
	s.leaderEndOffset = leaderEndOffset
	if s.recompute(time.Now()) {
		s.releaseWaiters()
	}
}

// WaitForHighWatermark blocks until the high watermark reaches target or the
// timeout fires.
//
// ponytail: a parked waiter is only re-evaluated on RecordFollowerFetch. There
// is no background ISR sweep, so a follower that dies mid-wait doesn't shrink
// the ISR until something else calls a recompute path — the waiting produce
// times out instead of completing once the ISR would have dropped it. Real
// Kafka runs a periodic replica-lag check. Fine for the acks=all demo, where
// the slow follower is alive and keeps fetching.
func (s *PartitionState) WaitForHighWatermark(target int64, timeout time.Duration) error {
	s.mu.Lock()
	if target > s.leaderEndOffset {
		s.leaderEndOffset = target
	}
	if s.recompute(time.Now()) {
		s.releaseWaiters()
	}
	if s.highWatermark >= target {
		s.mu.Unlock()
		return nil
	}
	waiter := &hwWaiter{target: target, done: make(chan struct{})}
	s.waiters = append(s.waiters, waiter)
	done := waiter.done
	s.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		s.mu.Lock()
		s.removeWaiter(waiter)
		s.mu.Unlock()
		return errHighWatermarkTimedOut
	}
}

func (s *PartitionState) HighWatermark(leaderEndOffset int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leaderEndOffset = leaderEndOffset
	if s.recompute(time.Now()) {
		s.releaseWaiters()
	}
	return s.highWatermark
}

func (s *PartitionState) ISR() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recompute(time.Now())
	return s.isr(time.Now())
}

func (s *PartitionState) recompute(now time.Time) bool {
	next := s.leaderEndOffset
	for _, brokerID := range s.replicas {
		if brokerID == s.leaderID {
			continue
		}
		if !s.followerInISR(brokerID, now) {
			continue
		}
		if offset := s.followerOffset[brokerID]; offset < next {
			next = offset
		}
	}
	changed := next != s.highWatermark
	s.highWatermark = next
	return changed
}

func (s *PartitionState) followerInISR(brokerID int32, now time.Time) bool {
	seen, ok := s.followerSeen[brokerID]
	if !ok {
		return false
	}
	return now.Sub(seen) <= s.lagTimeout
}

func (s *PartitionState) isr(now time.Time) []int32 {
	out := []int32{s.leaderID}
	for _, brokerID := range s.replicas {
		if brokerID == s.leaderID {
			continue
		}
		if s.followerInISR(brokerID, now) {
			out = append(out, brokerID)
		}
	}
	return out
}

func (s *PartitionState) releaseWaiters() {
	remaining := s.waiters[:0]
	for _, waiter := range s.waiters {
		if waiter.target <= s.highWatermark {
			close(waiter.done)
			continue
		}
		remaining = append(remaining, waiter)
	}
	s.waiters = remaining
}

func (s *PartitionState) removeWaiter(want *hwWaiter) {
	remaining := s.waiters[:0]
	for _, waiter := range s.waiters {
		if waiter == want {
			continue
		}
		remaining = append(remaining, waiter)
	}
	s.waiters = remaining
}
