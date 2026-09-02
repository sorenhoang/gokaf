package offset

import (
	"bytes"
	"sync"
	"time"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
)

type key struct {
	group     string
	topic     string
	partition int32
}

type Store struct {
	mu      sync.RWMutex
	log     *storage.Log
	offsets map[key]int64
}

func NewStore(log *storage.Log) (*Store, error) {
	store := &Store{
		log:     log,
		offsets: map[key]int64{},
	}
	if err := store.replay(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Commit(group string, topic string, partition int32, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// ponytail: append under the store lock. Commits are rare, and serialized
	// ordering gives replay last-write-wins without extra coordination.
	if _, err := s.log.Append(encodeEntry(group, topic, partition, offset)); err != nil {
		return err
	}
	s.offsets[key{group: group, topic: topic, partition: partition}] = offset
	return nil
}

func (s *Store) Fetch(group string, topic string, partition int32) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.offsets[key{group: group, topic: topic, partition: partition}]
	if !ok {
		return -1
	}
	return offset
}

func (s *Store) replay() error {
	next := int64(0)
	for {
		records, err := s.log.Read(next, -1)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			group, topic, partition, offset, err := decodeEntry(record.Payload)
			if err != nil {
				return err
			}
			s.offsets[key{group: group, topic: topic, partition: partition}] = offset
			next = record.Offset + 1
		}
	}
}

func encodeEntry(group string, topic string, partition int32, offset int64) []byte {
	e := protocol.NewEncoder()
	e.WriteString(group)
	e.WriteString(topic)
	e.WriteInt32(partition)
	e.WriteInt64(offset)
	// ponytail: commit timestamp is persisted but nothing reads it yet. Real
	// Kafka uses it for offset retention (offsets.retention.minutes).
	e.WriteInt64(time.Now().UnixMilli())
	return e.Bytes()
}

func decodeEntry(payload []byte) (string, string, int32, int64, error) {
	dec := protocol.NewDecoder(bytes.NewReader(payload))
	group, err := dec.ReadString()
	if err != nil {
		return "", "", 0, 0, err
	}
	topic, err := dec.ReadString()
	if err != nil {
		return "", "", 0, 0, err
	}
	partition, err := dec.ReadInt32()
	if err != nil {
		return "", "", 0, 0, err
	}
	offset, err := dec.ReadInt64()
	if err != nil {
		return "", "", 0, 0, err
	}
	if _, err := dec.ReadInt64(); err != nil {
		return "", "", 0, 0, err
	}
	return group, topic, partition, offset, nil
}
