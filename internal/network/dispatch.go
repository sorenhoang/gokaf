package network

import (
	"bytes"
	"io"
	"log"
	"net"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/offset"
	"github.com/sorenhoang/gokaf/internal/producer"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/replication"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

// handlerFunc is a method expression: the first argument is the receiver, so
// the dispatch table can be built once at package load instead of per request.
type handlerFunc func(*Broker, protocol.RequestHeader, []byte) ([]byte, error)

var dispatchTable = map[int16]handlerFunc{
	0:    (*Broker).handleProduce,
	1:    (*Broker).handleFetch,
	2:    (*Broker).handleListOffsets,
	3:    (*Broker).handleMetadata,
	8:    (*Broker).handleOffsetCommit,
	9:    (*Broker).handleOffsetFetch,
	10:   (*Broker).handleFindCoordinator,
	11:   (*Broker).handleJoinGroup,
	12:   (*Broker).handleHeartbeat,
	13:   (*Broker).handleLeaveGroup,
	14:   (*Broker).handleSyncGroup,
	18:   (*Broker).handleApiVersions,
	19:   (*Broker).handleCreateTopics,
	20:   (*Broker).handleDeleteTopics,
	22:   (*Broker).handleInitProducerID,
	1000: (*Broker).handleApplyTopic, // internalApplyTopicKey
	1001: (*Broker).handlePing,       // cluster.InternalPingKey
	1002: (*Broker).handleFetchMetadataLog,
}

type Broker struct {
	NodeID       int32
	Host         string
	Port         int32
	Topics       *topic.Registry
	Logs         *storage.Manager
	Groups       *group.Coordinator
	Offsets      *offset.Store
	Producers    *producer.Manager
	Cluster      *cluster.Membership
	Replication  *replication.Manager
	IsPeerAlive  func(int32) bool
	ControllerID func() int32
	MetadataLog  *cluster.MetadataLog
}

func (b *Broker) Dispatch(conn net.Conn, payload []byte) error {
	reader := bytes.NewReader(payload)
	dec := protocol.NewDecoder(reader)

	header, err := protocol.ReadRequestHeader(dec)
	if err != nil {
		log.Printf("failed to read request header from %s: %v", conn.RemoteAddr(), err)
		return err
	}

	body := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, body); err != nil {
		log.Printf("failed to read request body from %s: %v", conn.RemoteAddr(), err)
		return err
	}

	handler, ok := dispatchTable[header.APIKey]
	var responseBody []byte
	if ok {
		responseBody, err = handler(b, header, body)
		if err != nil {
			return err
		}
	} else {
		e := protocol.NewEncoder()
		e.WriteInt16(-1)
		responseBody = e.Bytes()
	}

	e := protocol.NewEncoder()
	protocol.WriteResponseHeader(e, protocol.ResponseHeader{CorrelationID: header.CorrelationID})
	response := append(e.Bytes(), responseBody...)
	return WriteFrame(conn, response)
}
