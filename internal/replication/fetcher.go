package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
)

type fetcher struct {
	topic     string
	partition int32
	selfID    int32
	leader    cluster.Broker
	localLog  *storage.Log
	interval  time.Duration
	maxBytes  int32
}

func (f *fetcher) run(ctx context.Context) {
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

func (f *fetcher) fetchOnce() {
	from := f.localLog.EndOffset()
	body := buildFetchRequest(f.selfID, f.topic, f.partition, from, f.maxBytes)
	resp, err := cluster.NewBrokerClient(f.leader).Send(protocol.RequestHeader{APIKey: 1, APIVersion: 0, CorrelationID: 1}, body)
	if err != nil {
		log.Printf("replica %s-%d: fetch from broker %d: %v", f.topic, f.partition, f.leader.ID, err)
		return
	}

	batchBytes, errCode, err := parseFetchPartition(resp)
	if err != nil {
		log.Printf("replica %s-%d: parse fetch: %v", f.topic, f.partition, err)
		return
	}
	if errCode != protocol.ErrNone || len(batchBytes) == 0 {
		return
	}

	for _, batch := range splitBatches(batchBytes) {
		if _, err := f.localLog.Append(batch); err != nil {
			log.Printf("replica %s-%d: append: %v", f.topic, f.partition, err)
			return
		}
	}
}

func buildFetchRequest(replicaID int32, topicName string, partition int32, offset int64, maxBytes int32) []byte {
	e := protocol.NewEncoder()
	e.WriteInt32(replicaID)
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteArrayLen(1)
	e.WriteString(topicName)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(offset)
	e.WriteInt32(maxBytes)
	return e.Bytes()
}

func parseFetchPartition(resp []byte) ([]byte, int16, error) {
	dec := protocol.NewDecoder(bytes.NewReader(resp))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, protocol.ErrUnknown, err
	}
	if topicCount == 0 {
		return nil, protocol.ErrUnknown, nil
	}
	if _, err := dec.ReadString(); err != nil {
		return nil, protocol.ErrUnknown, err
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, protocol.ErrUnknown, err
	}
	if partitionCount == 0 {
		return nil, protocol.ErrUnknown, nil
	}
	if _, err := dec.ReadInt32(); err != nil {
		return nil, protocol.ErrUnknown, err
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		return nil, protocol.ErrUnknown, err
	}
	if _, err := dec.ReadInt64(); err != nil {
		return nil, protocol.ErrUnknown, err
	}
	batches, err := dec.ReadBytes()
	if err != nil {
		return nil, protocol.ErrUnknown, err
	}
	return batches, errorCode, nil
}

func splitBatches(b []byte) [][]byte {
	var out [][]byte
	for len(b) >= 12 {
		total := 12 + int(binary.BigEndian.Uint32(b[8:12]))
		if total > len(b) || total < 12 {
			break
		}
		out = append(out, b[:total])
		b = b[total:]
	}
	return out
}
