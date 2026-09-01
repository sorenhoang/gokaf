package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleFindCoordinatorReturnsThisBroker(t *testing.T) {
	broker := &Broker{NodeID: 1, Host: "localhost", Port: 9092}

	req := protocol.NewEncoder()
	req.WriteString("my-group")

	body, err := broker.handleFindCoordinator(protocol.RequestHeader{APIKey: 10, APIVersion: 0}, req.Bytes())
	if err != nil {
		t.Fatalf("handleFindCoordinator: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	nodeID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 node id: unexpected error: %v", err)
	}
	host, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString host: unexpected error: %v", err)
	}
	port, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 port: unexpected error: %v", err)
	}

	if errorCode != protocol.ErrNone || nodeID != 1 || host != "localhost" || port != 9092 {
		t.Fatalf("coordinator: got {error=%d, node_id=%d, host=%q, port=%d}, want {0, 1, %q, 9092}", errorCode, nodeID, host, port, "localhost")
	}
}

func TestFindCoordinatorHandlerIsRegistered(t *testing.T) {
	if dispatchTable[10] == nil {
		t.Fatal("FindCoordinator handler for api_key 10 is not registered")
	}
}
