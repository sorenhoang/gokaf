package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

const InternalPingKey int16 = 1001

type LivenessMonitor struct {
	self       int32
	membership *Membership
	interval   time.Duration
	failAfter  int
	onDown     func(int32)
	onUp       func(int32)
	timeout    time.Duration

	mu     sync.RWMutex
	alive  map[int32]bool
	misses map[int32]int
}

func NewLivenessMonitor(m *Membership, self int32, interval time.Duration, failAfter int, onDown, onUp func(int32)) *LivenessMonitor {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if failAfter <= 0 {
		failAfter = 1
	}
	alive := map[int32]bool{self: true}
	for _, broker := range m.All() {
		alive[broker.ID] = true
	}
	return &LivenessMonitor{
		self:       self,
		membership: m,
		interval:   interval,
		failAfter:  failAfter,
		onDown:     onDown,
		onUp:       onUp,
		timeout:    200 * time.Millisecond,
		alive:      alive,
		misses:     map[int32]int{},
	}
}

func (lm *LivenessMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(lm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.checkPeers()
		}
	}
}

func (lm *LivenessMonitor) Alive(id int32) bool {
	if id == lm.self {
		return true
	}
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.alive[id]
}

func (lm *LivenessMonitor) checkPeers() {
	for _, broker := range lm.membership.All() {
		if broker.ID == lm.self {
			continue
		}
		if err := lm.ping(broker); err != nil {
			lm.recordMiss(broker.ID)
			continue
		}
		lm.recordSuccess(broker.ID)
	}
}

func (lm *LivenessMonitor) ping(broker Broker) error {
	addr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
	conn, err := net.DialTimeout("tcp", addr, lm.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(lm.timeout)); err != nil {
		return err
	}

	header := protocol.RequestHeader{APIKey: InternalPingKey, APIVersion: 0, CorrelationID: 1}
	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	if err := protocol.WriteFrame(conn, e.Bytes()); err != nil {
		return err
	}
	respPayload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		return err
	}
	if respHeader.CorrelationID != header.CorrelationID {
		return fmt.Errorf("correlation id: got %d, want %d", respHeader.CorrelationID, header.CorrelationID)
	}
	return nil
}

func (lm *LivenessMonitor) recordMiss(id int32) {
	var fireDown bool
	lm.mu.Lock()
	lm.misses[id]++
	if lm.misses[id] >= lm.failAfter && lm.alive[id] {
		lm.alive[id] = false
		fireDown = true
	}
	lm.mu.Unlock()

	if fireDown && lm.onDown != nil {
		lm.onDown(id)
	}
}

func (lm *LivenessMonitor) recordSuccess(id int32) {
	var fireUp bool
	lm.mu.Lock()
	wasAlive := lm.alive[id]
	lm.misses[id] = 0
	lm.alive[id] = true
	if !wasAlive {
		fireUp = true
	}
	lm.mu.Unlock()

	if fireUp && lm.onUp != nil {
		lm.onUp(id)
	}
}
