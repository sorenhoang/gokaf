package network

import (
	"testing"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandlePingReturnsEmptySuccessBody(t *testing.T) {
	broker := &Broker{}

	body, err := broker.handlePing(protocol.RequestHeader{APIKey: cluster.InternalPingKey}, nil)
	if err != nil {
		t.Fatalf("handlePing: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("ping body length = %d, want 0", len(body))
	}
}

func TestPingHandlerIsRegistered(t *testing.T) {
	if dispatchTable[cluster.InternalPingKey] == nil {
		t.Fatal("Ping handler for api_key 1001 is not registered")
	}
}
