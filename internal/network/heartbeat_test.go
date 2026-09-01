package network

import (
	"bytes"
	"testing"
	"time"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleHeartbeatWritesErrorCode(t *testing.T) {
	coordinator := group.NewCoordinator(time.Millisecond)
	joinResult := coordinator.Join(group.JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 500,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []group.Protocol{{Name: "range", Metadata: []byte("subscription")}},
	})
	coordinator.Sync(group.SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: joinResult.GenerationID,
		MemberID:     joinResult.MemberID,
		Assignments:  map[string][]byte{joinResult.MemberID: []byte("assignment")},
	})
	broker := &Broker{Groups: coordinator}

	body, err := broker.handleHeartbeat(protocol.RequestHeader{APIKey: 12, APIVersion: 0}, heartbeatRequest(joinResult.GenerationID, joinResult.MemberID))
	if err != nil {
		t.Fatalf("handleHeartbeat: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if errorCode != protocol.ErrNone {
		t.Fatalf("heartbeat error code: got %d, want 0", errorCode)
	}
}

func TestHeartbeatHandlerIsRegistered(t *testing.T) {
	if dispatchTable[12] == nil {
		t.Fatal("Heartbeat handler for api_key 12 is not registered")
	}
}

func heartbeatRequest(generationID int32, memberID string) []byte {
	e := protocol.NewEncoder()
	e.WriteString("orders-consumers")
	e.WriteInt32(generationID)
	e.WriteString(memberID)
	return e.Bytes()
}
