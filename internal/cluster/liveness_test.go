package cluster

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestLivenessMonitorMarksPeerDownAfterConsecutiveMisses(t *testing.T) {
	port := unusedPort(t)
	membership, err := ParseMembership("1@localhost:19092,2@localhost:"+port, 1, "localhost", 19092)
	if err != nil {
		t.Fatalf("ParseMembership: %v", err)
	}
	down := make(chan int32, 1)
	lm := NewLivenessMonitor(membership, 1, 10*time.Millisecond, 2, func(id int32) {
		down <- id
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go lm.Run(ctx)

	select {
	case got := <-down:
		if got != 2 {
			t.Fatalf("down peer = %d, want 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("peer was not marked down")
	}
	if lm.Alive(2) {
		t.Fatal("peer 2 is still alive after failAfter misses")
	}
}

func unusedPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close listener: %v", err)
	}
	return port
}
