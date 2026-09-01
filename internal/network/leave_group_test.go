package network

import (
	"bytes"
	"testing"
	"time"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleLeaveGroupWritesErrorCode(t *testing.T) {
	coordinator := group.NewCoordinator(time.Millisecond)
	joinResult := coordinator.Join(group.JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 500,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []group.Protocol{{Name: "range", Metadata: []byte("subscription")}},
	})
	broker := &Broker{Groups: coordinator}

	body, err := broker.handleLeaveGroup(protocol.RequestHeader{APIKey: 13, APIVersion: 0}, leaveGroupRequest(joinResult.MemberID))
	if err != nil {
		t.Fatalf("handleLeaveGroup: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if errorCode != protocol.ErrNone {
		t.Fatalf("leave error code: got %d, want 0", errorCode)
	}
}

func TestLeaveGroupHandlerIsRegistered(t *testing.T) {
	if dispatchTable[13] == nil {
		t.Fatal("LeaveGroup handler for api_key 13 is not registered")
	}
}

func leaveGroupRequest(memberID string) []byte {
	e := protocol.NewEncoder()
	e.WriteString("orders-consumers")
	e.WriteString(memberID)
	return e.Bytes()
}
