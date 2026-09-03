package network

import (
	"bytes"
	"errors"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

type topicResult struct {
	name string
	code int16
}

func (b *Broker) handleDeleteTopics(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	results := make([]topicResult, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		name, err := dec.ReadString()
		if err != nil {
			return nil, err
		}

		if b.MetadataLog != nil && b.controllerID() != b.NodeID {
			results = append(results, topicResult{name: name, code: protocol.ErrNotController})
			continue
		}
		results = append(results, topicResult{
			name: name,
			code: b.deleteTopic(name),
		})
	}

	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	writeTopicResults(e, results)
	return e.Bytes(), nil
}

func (b *Broker) deleteTopic(name string) int16 {
	if b.MetadataLog != nil {
		if _, ok := b.Topics.Get(name); !ok {
			return protocol.ErrUnknownTopicOrPartition
		}
		if _, err := b.MetadataLog.Append(cluster.Record{Type: cluster.TopicDelete, Topic: name}); err != nil {
			return protocol.ErrUnknown
		}
		b.ApplyMetadataRecord(cluster.Record{Type: cluster.TopicDelete, Topic: name})
		return protocol.ErrNone
	}
	err := b.Topics.Delete(name)
	switch {
	case errors.Is(err, topic.ErrTopicNotFound):
		return protocol.ErrUnknownTopicOrPartition
	case err != nil:
		return protocol.ErrUnknown
	default:
		return protocol.ErrNone
	}
}

func writeTopicResults(e *protocol.Encoder, results []topicResult) {
	e.WriteArrayLen(len(results))
	for _, result := range results {
		e.WriteString(result.name)
		e.WriteInt16(result.code)
	}
}
