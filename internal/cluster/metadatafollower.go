package cluster

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

const InternalFetchMetadataLogKey int16 = 1002

type MetadataFollower struct {
	metadataLog *MetadataLog
	membership  *Membership
	selfID      int32
	controller  func() int32
	apply       func(Record)
	interval    time.Duration
}

func NewMetadataFollower(metadataLog *MetadataLog, membership *Membership, selfID int32, controller func() int32, apply func(Record), interval time.Duration) *MetadataFollower {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &MetadataFollower{metadataLog: metadataLog, membership: membership, selfID: selfID, controller: controller, apply: apply, interval: interval}
}

func (f *MetadataFollower) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.fetchOnce()
		}
	}
}

// fetchOnce pulls new metadata records from the current controller and applies
// them. A broker that is the controller pulls from nobody.
//
// ponytail: not Raft. If a broker was down while the controller wrote records
// and then restarts as the highest live id, it becomes controller with a stale
// log and never back-fills — its metadata diverges from the peers that stayed
// up. A full simultaneous restart (the phase AC) is fine because everyone
// replays their own complete local log. Real safety needs a real consensus
// log with a leader-completeness guarantee.
func (f *MetadataFollower) fetchOnce() {
	controllerID := f.selfID
	if f.controller != nil {
		controllerID = f.controller()
	}
	if controllerID == f.selfID {
		return
	}
	controller, ok := f.membership.Get(controllerID)
	if !ok {
		return
	}

	e := protocol.NewEncoder()
	e.WriteInt64(f.metadataLog.EndOffset())
	response, err := NewBrokerClient(controller).Send(protocol.RequestHeader{APIKey: InternalFetchMetadataLogKey, APIVersion: 0, CorrelationID: 1}, e.Bytes())
	if err != nil {
		return
	}
	dec := protocol.NewDecoder(bytes.NewReader(response))
	count, err := dec.ReadArrayLen()
	if err != nil {
		log.Printf("metadata follower: decode record count: %v", err)
		return
	}
	for i := 0; i < count; i++ {
		payload, err := dec.ReadBytes()
		if err != nil {
			log.Printf("metadata follower: decode record: %v", err)
			return
		}
		if _, err := f.metadataLog.AppendRaw(payload); err != nil {
			log.Printf("metadata follower: append: %v", err)
			return
		}
		record, err := decodeMetadataRecord(payload)
		if err != nil {
			log.Printf("metadata follower: decode record payload: %v", err)
			return
		}
		if f.apply != nil {
			f.apply(record)
		}
	}
}
