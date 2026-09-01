package network

import (
	"bytes"
	"testing"
	"time"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleJoinGroupWritesLeaderResponse(t *testing.T) {
	broker := &Broker{Groups: group.NewCoordinator(time.Millisecond)}

	body, err := broker.handleJoinGroup(protocol.RequestHeader{APIKey: 11, APIVersion: 0}, joinGroupRequest("", "range", []byte("subscription")))
	if err != nil {
		t.Fatalf("handleJoinGroup: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	generationID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 generation id: unexpected error: %v", err)
	}
	protocolName, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString protocol name: unexpected error: %v", err)
	}
	leaderID, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString leader id: unexpected error: %v", err)
	}
	memberID, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString member id: unexpected error: %v", err)
	}
	memberCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen members: unexpected error: %v", err)
	}
	if errorCode != protocol.ErrNone || generationID != 1 || protocolName != "range" || leaderID == "" || leaderID != memberID || memberCount != 1 {
		t.Fatalf("join response: got {error=%d generation=%d protocol=%q leader=%q member=%q members=%d}, want {0 1 range same same 1}", errorCode, generationID, protocolName, leaderID, memberID, memberCount)
	}
	gotMemberID, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString join member id: unexpected error: %v", err)
	}
	metadata, err := dec.ReadBytes()
	if err != nil {
		t.Fatalf("ReadBytes metadata: unexpected error: %v", err)
	}
	if gotMemberID != memberID || string(metadata) != "subscription" {
		t.Fatalf("join member: got {%q, %q}, want {%q, subscription}", gotMemberID, metadata, memberID)
	}
}

func TestJoinGroupHandlerIsRegistered(t *testing.T) {
	if dispatchTable[11] == nil {
		t.Fatal("JoinGroup handler for api_key 11 is not registered")
	}
}

func joinGroupRequest(memberID string, protocolName string, metadata []byte) []byte {
	e := protocol.NewEncoder()
	e.WriteString("orders-consumers")
	e.WriteInt32(30000)
	e.WriteString(memberID)
	e.WriteString("consumer")
	e.WriteArrayLen(1)
	e.WriteString(protocolName)
	e.WriteBytes(metadata)
	return e.Bytes()
}
