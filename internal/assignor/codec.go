package assignor

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

// EncodeAssignment serialises a member's assignment into the blob the
// coordinator moves through SyncGroup: version, then a topic array each with
// its partition list, then a null user-data field.
func EncodeAssignment(assignments []TopicPartitions) []byte {
	e := protocol.NewEncoder()
	e.WriteInt16(0)
	e.WriteArrayLen(len(assignments))
	for _, a := range assignments {
		e.WriteString(a.Topic)
		e.WriteArrayLen(len(a.Partitions))
		for _, p := range a.Partitions {
			e.WriteInt32(p)
		}
	}
	e.WriteInt32(-1)
	return e.Bytes()
}

// DecodeAssignment is the inverse of EncodeAssignment. A nil or empty blob
// decodes to no assignments.
func DecodeAssignment(b []byte) ([]TopicPartitions, error) {
	if len(b) == 0 {
		return nil, nil
	}
	dec := protocol.NewDecoder(bytes.NewReader(b))
	if _, err := dec.ReadInt16(); err != nil {
		return nil, err
	}
	count, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	out := make([]TopicPartitions, 0, count)
	for i := 0; i < count; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		partitions := make([]int32, 0, partitionCount)
		for j := 0; j < partitionCount; j++ {
			p, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, p)
		}
		out = append(out, TopicPartitions{Topic: topicName, Partitions: partitions})
	}
	return out, nil
}
