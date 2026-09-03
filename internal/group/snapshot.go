package group

// GroupSnapshot is a read-only view of one consumer group for the admin API.
type GroupSnapshot struct {
	ID           string
	State        string
	GenerationID int32
	LeaderID     string
	Protocol     string
	Members      []MemberSnapshot
}

// MemberSnapshot carries a member's id and its current assignment blob.
type MemberSnapshot struct {
	ID         string
	Assignment []byte
}

// Snapshot returns a copy of every group's current state, ordered by group id
// is not guaranteed — the caller sorts if it needs stable output.
func (c *Coordinator) Snapshot() []GroupSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]GroupSnapshot, 0, len(c.groups))
	for _, g := range c.groups {
		members := make([]MemberSnapshot, 0, len(g.JoinOrder))
		for _, id := range g.JoinOrder {
			m, ok := g.Members[id]
			if !ok {
				continue
			}
			members = append(members, MemberSnapshot{ID: m.ID, Assignment: cloneBytes(m.Assignment)})
		}
		out = append(out, GroupSnapshot{
			ID:           g.ID,
			State:        g.State.String(),
			GenerationID: g.GenerationID,
			LeaderID:     g.LeaderID,
			Protocol:     g.Protocol,
			Members:      members,
		})
	}
	return out
}
