package group

import (
	"slices"
	"testing"
	"time"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestJoinCompletesRebalanceAndLeaderGetsMemberMetadata(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)

	firstCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 30000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-a")}},
	})
	time.Sleep(time.Millisecond)
	secondCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-b",
		SessionTimeoutMS: 30000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-b")}},
	})

	first := receiveJoin(t, firstCh)
	second := receiveJoin(t, secondCh)

	if first.ErrorCode != protocol.ErrNone || second.ErrorCode != protocol.ErrNone {
		t.Fatalf("join error codes: got %d/%d, want 0/0", first.ErrorCode, second.ErrorCode)
	}
	if first.GenerationID != 1 || second.GenerationID != 1 {
		t.Fatalf("generation ids: got %d/%d, want 1/1", first.GenerationID, second.GenerationID)
	}
	if first.Protocol != "range" || second.Protocol != "range" {
		t.Fatalf("protocols: got %q/%q, want range/range", first.Protocol, second.Protocol)
	}
	if first.LeaderID != first.MemberID || second.LeaderID != first.MemberID {
		t.Fatalf("leader ids: got %q/%q, want first member %q", first.LeaderID, second.LeaderID, first.MemberID)
	}
	if first.MemberID == "" || second.MemberID == "" || first.MemberID == second.MemberID {
		t.Fatalf("member ids: got %q/%q, want distinct non-empty ids", first.MemberID, second.MemberID)
	}
	if len(first.Members) != 2 {
		t.Fatalf("leader member count: got %d, want 2", len(first.Members))
	}
	if len(second.Members) != 0 {
		t.Fatalf("follower member count: got %d, want 0", len(second.Members))
	}
	assertJoinMember(t, first.Members[0], first.MemberID, "sub-a")
	assertJoinMember(t, first.Members[1], second.MemberID, "sub-b")

	state := coordinator.State("orders-consumers")
	if state != CompletingRebalance {
		t.Fatalf("group state: got %s, want %s", state, CompletingRebalance)
	}
}

func TestSyncFollowerWaitsUntilLeaderDistributesAssignments(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, second := joinTwoMembers(t, coordinator)

	followerCh := syncAsync(coordinator, SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     second.MemberID,
		Assignments:  nil,
	})

	select {
	case result := <-followerCh:
		t.Fatalf("follower sync returned before leader assignment: %+v", result)
	case <-time.After(5 * time.Millisecond):
	}

	leaderResult := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.MemberID,
		Assignments: map[string][]byte{
			first.MemberID:  []byte("assign-a"),
			second.MemberID: []byte("assign-b"),
		},
	})
	if leaderResult.ErrorCode != protocol.ErrNone || string(leaderResult.Assignment) != "assign-a" {
		t.Fatalf("leader sync: got {error=%d assignment=%q}, want {0 assign-a}", leaderResult.ErrorCode, leaderResult.Assignment)
	}

	followerResult := receiveSync(t, followerCh)
	if followerResult.ErrorCode != protocol.ErrNone || string(followerResult.Assignment) != "assign-b" {
		t.Fatalf("follower sync: got {error=%d assignment=%q}, want {0 assign-b}", followerResult.ErrorCode, followerResult.Assignment)
	}

	if state := coordinator.State("orders-consumers"); state != Stable {
		t.Fatalf("group state: got %s, want %s", state, Stable)
	}
}

func TestSyncReturnsAssignmentImmediatelyAfterGroupIsStable(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, second := joinTwoMembers(t, coordinator)

	leaderResult := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.MemberID,
		Assignments: map[string][]byte{
			first.MemberID:  []byte("assign-a"),
			second.MemberID: []byte("assign-b"),
		},
	})
	if leaderResult.ErrorCode != protocol.ErrNone {
		t.Fatalf("leader sync error code: got %d, want 0", leaderResult.ErrorCode)
	}

	followerResult := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     second.MemberID,
		Assignments:  nil,
	})
	if followerResult.ErrorCode != protocol.ErrNone || string(followerResult.Assignment) != "assign-b" {
		t.Fatalf("follower sync: got {error=%d assignment=%q}, want {0 assign-b}", followerResult.ErrorCode, followerResult.Assignment)
	}
}

func TestSyncRejectsUnknownMemberAndIllegalGeneration(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, _ := joinTwoMembers(t, coordinator)

	unknown := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     "missing",
	})
	if unknown.ErrorCode != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown member error code: got %d, want %d", unknown.ErrorCode, protocol.ErrUnknownMemberID)
	}

	stale := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID - 1,
		MemberID:     first.MemberID,
	})
	if stale.ErrorCode != protocol.ErrIllegalGeneration {
		t.Fatalf("illegal generation error code: got %d, want %d", stale.ErrorCode, protocol.ErrIllegalGeneration)
	}
}

func joinTwoMembers(t *testing.T, coordinator *Coordinator) (JoinResult, JoinResult) {
	t.Helper()

	firstCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 30000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-a")}},
	})
	time.Sleep(time.Millisecond)
	secondCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-b",
		SessionTimeoutMS: 30000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-b")}},
	})

	return receiveJoin(t, firstCh), receiveJoin(t, secondCh)
}

func joinAsync(coordinator *Coordinator, req JoinRequest) <-chan JoinResult {
	ch := make(chan JoinResult, 1)
	go func() {
		ch <- coordinator.Join(req)
	}()
	return ch
}

func syncAsync(coordinator *Coordinator, req SyncRequest) <-chan SyncResult {
	ch := make(chan SyncResult, 1)
	go func() {
		ch <- coordinator.Sync(req)
	}()
	return ch
}

func receiveJoin(t *testing.T, ch <-chan JoinResult) JoinResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Join")
		return JoinResult{}
	}
}

func receiveSync(t *testing.T, ch <-chan SyncResult) SyncResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Sync")
		return SyncResult{}
	}
}

func assertJoinMember(t *testing.T, member JoinMember, wantID string, wantMetadata string) {
	t.Helper()

	if member.ID != wantID || !slices.Equal(member.Metadata, []byte(wantMetadata)) {
		t.Fatalf("join member: got {%q, %q}, want {%q, %q}", member.ID, member.Metadata, wantID, wantMetadata)
	}
}
