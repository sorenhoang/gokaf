package network

import (
	"log"
	"sort"
)

// OnPeerDown lets only the elected controller decide replacements for a dead
// leader. All other brokers wait for the controller's ApplyTopic message.
//
// ponytail: the "controller = highest live broker id" election is derived from
// ping-based liveness, not a quorum. Under a real network partition two sides
// could each elect their own controller and both reassign the same partition —
// split brain. The test kills whole processes so every broker sees the same
// liveness, and a recovered highest-id broker silently reclaims controllership
// (churn we accept). Phase 23's metadata log persists decisions but is still
// not Raft; real safety needs real consensus.
func (b *Broker) OnPeerDown(deadID int32) {
	if b.controllerID() != b.NodeID {
		return
	}
	log.Printf("controller %d reassigning partitions led by dead broker %d", b.NodeID, deadID)
	for _, t := range b.Topics.All() {
		changed := false
		for i := range t.Partitions {
			partition := &t.Partitions[i]
			if partition.Leader != deadID {
				continue
			}
			liveISR := b.liveISR(partition.ISR, deadID)
			if len(liveISR) == 0 {
				log.Printf("partition %s-%d unavailable: dead leader %d and empty live ISR", t.Name, partition.ID, deadID)
				continue
			}
			partition.Leader = liveISR[0]
			partition.ISR = liveISR
			changed = true
		}
		if changed {
			b.applyTopic(t.Name, t.Partitions)
			b.fanOutTopic(t.Name, t.Partitions)
		}
	}
}

func (b *Broker) controllerID() int32 {
	if b.ControllerID != nil {
		return b.ControllerID()
	}
	return b.NodeID
}

func (b *Broker) OnPeerUp(id int32) {
	log.Printf("peer %d is up", id)
}

func (b *Broker) liveISR(isr []int32, deadID int32) []int32 {
	live := make([]int32, 0, len(isr))
	for _, brokerID := range isr {
		if brokerID == deadID {
			continue
		}
		if !b.peerAlive(brokerID) {
			continue
		}
		live = append(live, brokerID)
	}
	sort.Slice(live, func(i, j int) bool {
		return live[i] < live[j]
	})
	return live
}

func (b *Broker) peerAlive(id int32) bool {
	if id == b.NodeID {
		return true
	}
	if b.IsPeerAlive == nil {
		return true
	}
	return b.IsPeerAlive(id)
}
