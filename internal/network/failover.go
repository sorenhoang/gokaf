package network

import (
	"log"
	"sort"
)

// OnPeerDown promotes this broker to leader of every partition whose leader
// just died, when this broker is the lowest-id live ISR member.
//
// ponytail: "lowest live ISR wins" is a bully rule, not a real election. Two
// brokers with divergent liveness views (a network partition) could both
// promote the same partition — split brain. The test starts and kills whole
// processes, so views stay consistent. Phase 22 replaces this with a single
// elected controller that owns every leadership decision.
func (b *Broker) OnPeerDown(deadID int32) {
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
			newLeader := liveISR[0]
			if newLeader != b.NodeID {
				continue
			}

			partition.Leader = b.NodeID
			partition.ISR = liveISR
			changed = true
			if b.Replication != nil {
				b.Replication.StopFollowing(t.Name, partition.ID)
				b.Replication.Lead(t.Name, partition.ID, liveISR)
			}
		}
		if changed {
			b.Topics.Upsert(t)
			b.fanOutTopic(t.Name, t.Partitions)
		}
	}
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
