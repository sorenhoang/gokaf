package group

import (
	"fmt"
	"sync"
	"time"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

type GroupState int

const (
	Empty GroupState = iota
	PreparingRebalance
	CompletingRebalance
	Stable
)

func (s GroupState) String() string {
	switch s {
	case Empty:
		return "Empty"
	case PreparingRebalance:
		return "PreparingRebalance"
	case CompletingRebalance:
		return "CompletingRebalance"
	case Stable:
		return "Stable"
	default:
		return fmt.Sprintf("GroupState(%d)", s)
	}
}

type Protocol struct {
	Name     string
	Metadata []byte
}

type JoinRequest struct {
	GroupID          string
	ClientID         string
	SessionTimeoutMS int32
	MemberID         string
	ProtocolType     string
	Protocols        []Protocol
}

type JoinMember struct {
	ID       string
	Metadata []byte
}

type JoinResult struct {
	GenerationID int32
	Protocol     string
	LeaderID     string
	MemberID     string
	Members      []JoinMember
	ErrorCode    int16
}

type SyncRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  map[string][]byte
}

type SyncResult struct {
	Assignment []byte
	ErrorCode  int16
}

type Coordinator struct {
	mu             sync.Mutex
	groups         map[string]*Group
	rebalanceDelay time.Duration
	nextMemberSeq  int64
}

type Group struct {
	ID           string
	State        GroupState
	GenerationID int32
	LeaderID     string
	Protocol     string
	Members      map[string]*Member
	JoinOrder    []string
	joinTimer    *time.Timer
}

type Member struct {
	ID               string
	Protocol         string
	Metadata         []byte
	Assignment       []byte
	SessionTimeoutMS int32
	awaitJoin        chan JoinResult
	awaitSync        chan SyncResult
}

func NewCoordinator(rebalanceDelay time.Duration) *Coordinator {
	return &Coordinator{
		groups:         map[string]*Group{},
		rebalanceDelay: rebalanceDelay,
	}
}

func (c *Coordinator) Join(req JoinRequest) JoinResult {
	c.mu.Lock()
	group := c.getOrCreateGroup(req.GroupID)
	memberID := req.MemberID
	if memberID == "" {
		memberID = c.nextMemberID(req.ClientID)
	}

	member, ok := group.Members[memberID]
	if !ok {
		member = &Member{ID: memberID}
		group.Members[memberID] = member
		group.JoinOrder = append(group.JoinOrder, memberID)
	}
	member.SessionTimeoutMS = req.SessionTimeoutMS
	member.Protocol = firstProtocolName(req.Protocols)
	member.Metadata = firstProtocolMetadata(req.Protocols)
	member.awaitJoin = make(chan JoinResult, 1)
	member.awaitSync = make(chan SyncResult, 1)

	if group.State != PreparingRebalance {
		group.State = PreparingRebalance
		group.joinTimer = time.AfterFunc(c.rebalanceDelay, func() {
			c.completeJoin(req.GroupID)
		})
	}
	ch := member.awaitJoin
	c.mu.Unlock()

	return <-ch
}

func (c *Coordinator) Sync(req SyncRequest) SyncResult {
	c.mu.Lock()
	group, ok := c.groups[req.GroupID]
	if !ok {
		c.mu.Unlock()
		return SyncResult{ErrorCode: protocol.ErrUnknownMemberID}
	}
	member, ok := group.Members[req.MemberID]
	if !ok {
		c.mu.Unlock()
		return SyncResult{ErrorCode: protocol.ErrUnknownMemberID}
	}
	if req.GenerationID != group.GenerationID {
		c.mu.Unlock()
		return SyncResult{ErrorCode: protocol.ErrIllegalGeneration}
	}

	if req.MemberID == group.LeaderID {
		for memberID, assignment := range req.Assignments {
			if target, ok := group.Members[memberID]; ok {
				target.Assignment = cloneBytes(assignment)
			}
		}
		group.State = Stable
		for _, waiting := range group.Members {
			if waiting.ID == member.ID {
				continue
			}
			select {
			case waiting.awaitSync <- SyncResult{Assignment: cloneBytes(waiting.Assignment), ErrorCode: protocol.ErrNone}:
			default:
			}
		}
		result := SyncResult{Assignment: cloneBytes(member.Assignment), ErrorCode: protocol.ErrNone}
		c.mu.Unlock()
		return result
	}

	if group.State == Stable {
		result := SyncResult{Assignment: cloneBytes(member.Assignment), ErrorCode: protocol.ErrNone}
		c.mu.Unlock()
		return result
	}

	ch := member.awaitSync
	c.mu.Unlock()
	return <-ch
}

func (c *Coordinator) State(groupID string) GroupState {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		return Empty
	}
	return group.State
}

func (c *Coordinator) completeJoin(groupID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok || len(group.JoinOrder) == 0 {
		return
	}
	group.LeaderID = group.JoinOrder[0]
	group.Protocol = groupProtocol(group)
	group.GenerationID++
	group.State = CompletingRebalance

	leaderMembers := make([]JoinMember, 0, len(group.JoinOrder))
	for _, memberID := range group.JoinOrder {
		member := group.Members[memberID]
		leaderMembers = append(leaderMembers, JoinMember{ID: member.ID, Metadata: cloneBytes(member.Metadata)})
	}

	for _, memberID := range group.JoinOrder {
		member := group.Members[memberID]
		result := JoinResult{
			GenerationID: group.GenerationID,
			Protocol:     group.Protocol,
			LeaderID:     group.LeaderID,
			MemberID:     member.ID,
			ErrorCode:    protocol.ErrNone,
		}
		if member.ID == group.LeaderID {
			result.Members = leaderMembers
		}
		// Non-blocking send, like the SyncGroup fan-out: awaitJoin is a fresh
		// cap-1 channel drained by the parked Join goroutine. A member left in
		// JoinOrder from a past generation whose connection is gone has no
		// reader; dropping its result is correct and keeps completeJoin from
		// stalling while it holds c.mu.
		select {
		case member.awaitJoin <- result:
		default:
		}
	}
}

func (c *Coordinator) getOrCreateGroup(groupID string) *Group {
	group, ok := c.groups[groupID]
	if ok {
		return group
	}
	group = &Group{
		ID:      groupID,
		State:   Empty,
		Members: map[string]*Member{},
	}
	c.groups[groupID] = group
	return group
}

func (c *Coordinator) nextMemberID(clientID string) string {
	c.nextMemberSeq++
	if clientID == "" {
		clientID = "member"
	}
	return fmt.Sprintf("%s-%d", clientID, c.nextMemberSeq)
}

func firstProtocolMetadata(protocols []Protocol) []byte {
	if len(protocols) == 0 {
		return nil
	}
	return cloneBytes(protocols[0].Metadata)
}

func firstProtocolName(protocols []Protocol) string {
	if len(protocols) == 0 {
		return ""
	}
	return protocols[0].Name
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// groupProtocol picks the assignment protocol for the rebalance.
//
// ponytail: just takes the first member's first protocol. Real Kafka
// intersects every member's supported-protocol list and fails the join with
// INCONSISTENT_GROUP_PROTOCOL when there's no common choice. Fine while every
// consumer sends "range"; Phase 15 does the real selection.
func groupProtocol(group *Group) string {
	if len(group.JoinOrder) == 0 {
		return ""
	}
	member := group.Members[group.JoinOrder[0]]
	return member.Protocol
}
