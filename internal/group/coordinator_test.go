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

func TestSessionTimeoutEvictsLastMemberAndLeavesGroupEmpty(t *testing.T) {
	coordinator := NewCoordinator(time.Millisecond)
	member := joinOneMember(t, coordinator, 120*time.Millisecond)
	stabilizeSingleMember(t, coordinator, member, []byte("assign-a"))

	time.Sleep(180 * time.Millisecond)

	if state := coordinator.State("orders-consumers"); state != Empty {
		t.Fatalf("group state: got %s, want %s", state, Empty)
	}
	if coordinator.hasMember("orders-consumers", member.MemberID) {
		t.Fatalf("expired member %q still exists", member.MemberID)
	}
}

func TestHeartbeatKeepsMemberSessionAlive(t *testing.T) {
	coordinator := NewCoordinator(time.Millisecond)
	member := joinOneMember(t, coordinator, 150*time.Millisecond)
	stabilizeSingleMember(t, coordinator, member, []byte("assign-a"))

	time.Sleep(90 * time.Millisecond)
	if code := coordinator.Heartbeat(HeartbeatRequest{
		GroupID:      "orders-consumers",
		GenerationID: member.GenerationID,
		MemberID:     member.MemberID,
	}); code != protocol.ErrNone {
		t.Fatalf("heartbeat error code: got %d, want 0", code)
	}
	time.Sleep(90 * time.Millisecond)

	if !coordinator.hasMember("orders-consumers", member.MemberID) {
		t.Fatalf("member %q expired even though heartbeat reset the session", member.MemberID)
	}
}

func TestExpiringOneStableMemberStartsRebalanceForSurvivor(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, second := joinTwoMembersWithTimeouts(t, coordinator, 500*time.Millisecond, 120*time.Millisecond)
	stabilizeTwoMembers(t, coordinator, first, second)

	coordinator.setRebalanceDelayForTest(100 * time.Millisecond)
	time.Sleep(180 * time.Millisecond)

	if coordinator.hasMember("orders-consumers", second.MemberID) {
		t.Fatalf("expired member %q still exists", second.MemberID)
	}
	if state := coordinator.State("orders-consumers"); state != PreparingRebalance {
		t.Fatalf("group state: got %s, want %s", state, PreparingRebalance)
	}
	if code := coordinator.Heartbeat(HeartbeatRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.MemberID,
	}); code != protocol.ErrRebalanceInProgress {
		t.Fatalf("survivor heartbeat error code: got %d, want %d", code, protocol.ErrRebalanceInProgress)
	}
}

func TestHeartbeatReturnsUnknownMemberRebalanceAndIllegalGeneration(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, second := joinTwoMembersWithTimeouts(t, coordinator, 500*time.Millisecond, 500*time.Millisecond)
	stabilizeTwoMembers(t, coordinator, first, second)

	unknown := coordinator.Heartbeat(HeartbeatRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     "missing",
	})
	if unknown != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown heartbeat code: got %d, want %d", unknown, protocol.ErrUnknownMemberID)
	}

	coordinator.setRebalanceDelayForTest(100 * time.Millisecond)
	coordinator.Leave(LeaveRequest{GroupID: "orders-consumers", MemberID: second.MemberID})
	rebalancing := coordinator.Heartbeat(HeartbeatRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.MemberID,
	})
	if rebalancing != protocol.ErrRebalanceInProgress {
		t.Fatalf("rebalancing heartbeat code: got %d, want %d", rebalancing, protocol.ErrRebalanceInProgress)
	}

	next := receiveJoin(t, joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: 500,
		MemberID:         first.MemberID,
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-a")}},
	}))
	stale := coordinator.Heartbeat(HeartbeatRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.MemberID,
	})
	if next.GenerationID != 2 || stale != protocol.ErrIllegalGeneration {
		t.Fatalf("stale heartbeat: next_generation=%d code=%d, want generation=2 code=%d", next.GenerationID, stale, protocol.ErrIllegalGeneration)
	}
}

func TestLeaveRemovesMemberAndStartsRebalance(t *testing.T) {
	coordinator := NewCoordinator(10 * time.Millisecond)
	first, second := joinTwoMembersWithTimeouts(t, coordinator, 500*time.Millisecond, 500*time.Millisecond)
	stabilizeTwoMembers(t, coordinator, first, second)

	coordinator.setRebalanceDelayForTest(100 * time.Millisecond)
	if code := coordinator.Leave(LeaveRequest{GroupID: "orders-consumers", MemberID: second.MemberID}); code != protocol.ErrNone {
		t.Fatalf("leave error code: got %d, want 0", code)
	}

	if coordinator.hasMember("orders-consumers", second.MemberID) {
		t.Fatalf("left member %q still exists", second.MemberID)
	}
	if state := coordinator.State("orders-consumers"); state != PreparingRebalance {
		t.Fatalf("group state: got %s, want %s", state, PreparingRebalance)
	}
}

func joinTwoMembers(t *testing.T, coordinator *Coordinator) (JoinResult, JoinResult) {
	t.Helper()
	return joinTwoMembersWithTimeouts(t, coordinator, 30*time.Second, 30*time.Second)
}

func joinTwoMembersWithTimeouts(t *testing.T, coordinator *Coordinator, firstTimeout time.Duration, secondTimeout time.Duration) (JoinResult, JoinResult) {
	t.Helper()
	firstCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: int32(firstTimeout / time.Millisecond),
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-a")}},
	})
	time.Sleep(time.Millisecond)
	secondCh := joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-b",
		SessionTimeoutMS: int32(secondTimeout / time.Millisecond),
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-b")}},
	})

	return receiveJoin(t, firstCh), receiveJoin(t, secondCh)
}

func joinOneMember(t *testing.T, coordinator *Coordinator, timeout time.Duration) JoinResult {
	t.Helper()

	return receiveJoin(t, joinAsync(coordinator, JoinRequest{
		GroupID:          "orders-consumers",
		ClientID:         "client-a",
		SessionTimeoutMS: int32(timeout / time.Millisecond),
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []Protocol{{Name: "range", Metadata: []byte("sub-a")}},
	}))
}

func stabilizeSingleMember(t *testing.T, coordinator *Coordinator, member JoinResult, assignment []byte) {
	t.Helper()

	result := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: member.GenerationID,
		MemberID:     member.MemberID,
		Assignments:  map[string][]byte{member.MemberID: assignment},
	})
	if result.ErrorCode != protocol.ErrNone {
		t.Fatalf("sync error code: got %d, want 0", result.ErrorCode)
	}
}

func stabilizeTwoMembers(t *testing.T, coordinator *Coordinator, first JoinResult, second JoinResult) {
	t.Helper()

	result := coordinator.Sync(SyncRequest{
		GroupID:      "orders-consumers",
		GenerationID: first.GenerationID,
		MemberID:     first.LeaderID,
		Assignments: map[string][]byte{
			first.MemberID:  []byte("assign-a"),
			second.MemberID: []byte("assign-b"),
		},
	})
	if result.ErrorCode != protocol.ErrNone {
		t.Fatalf("sync error code: got %d, want 0", result.ErrorCode)
	}
}

func (c *Coordinator) setRebalanceDelayForTest(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceDelay = d
}

func (c *Coordinator) hasMember(groupID string, memberID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		return false
	}
	_, ok = group.Members[memberID]
	return ok
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
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for Join")
		return JoinResult{}
	}
}

func receiveSync(t *testing.T, ch <-chan SyncResult) SyncResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(500 * time.Millisecond):
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
