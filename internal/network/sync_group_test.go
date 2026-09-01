package network

import (
	"bytes"
	"testing"
	"time"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleSyncGroupWritesMemberAssignment(t *testing.T) {
	coordinator := group.NewCoordinator(time.Millisecond)
	joinResult := coordinator.Join(group.JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 30000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []group.Protocol{{Name: "range", Metadata: []byte("subscription")}},
	})
	broker := &Broker{Groups: coordinator}

	body, err := broker.handleSyncGroup(protocol.RequestHeader{APIKey: 14, APIVersion: 0}, syncGroupRequest(joinResult.GenerationID, joinResult.MemberID, map[string][]byte{
		joinResult.MemberID: []byte("assignment"),
	}))
	if err != nil {
		t.Fatalf("handleSyncGroup: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	assignment, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes assignment: unexpected error: %v", err)
	}
	if errorCode != protocol.ErrNone || string(assignment) != "assignment" {
		t.Fatalf("sync response: got {error=%d assignment=%q}, want {0 assignment}", errorCode, assignment)
	}
}

func TestSyncGroupHandlerIsRegistered(t *testing.T) {
	if dispatchTable[14] == nil {
		t.Fatal("SyncGroup handler for api_key 14 is not registered")
	}
}

func syncGroupRequest(generationID int32, memberID string, assignments map[string][]byte) []byte {
	e := protocol.NewEncoder()
	e.WriteString("orders-consumers")
	e.WriteInt32(generationID)
	e.WriteString(memberID)
	e.WriteArrayLen(len(assignments))
	for assignmentMemberID, assignment := range assignments {
		e.WriteString(assignmentMemberID)
		e.WriteBytes(assignment)
	}
	return e.Bytes()
}
