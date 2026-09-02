package cluster

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
)

type Broker struct {
	ID   int32
	Host string
	Port int32
}

type Membership struct {
	self    int32
	brokers []Broker
}

func ParseMembership(peers string, selfID int32, selfHost string, selfPort int32) (*Membership, error) {
	if strings.TrimSpace(peers) == "" {
		return &Membership{
			self:    selfID,
			brokers: []Broker{{ID: selfID, Host: selfHost, Port: selfPort}},
		}, nil
	}

	tokens := strings.Split(peers, ",")
	brokers := make([]Broker, 0, len(tokens))
	seenSelf := false
	for _, token := range tokens {
		broker, err := parseBroker(strings.TrimSpace(token))
		if err != nil {
			return nil, err
		}
		if broker.ID == selfID {
			seenSelf = true
		}
		brokers = append(brokers, broker)
	}
	if !seenSelf {
		return nil, fmt.Errorf("peers does not include self broker id %d", selfID)
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})
	return &Membership{self: selfID, brokers: brokers}, nil
}

func (m *Membership) All() []Broker {
	brokers := make([]Broker, len(m.brokers))
	copy(brokers, m.brokers)
	return brokers
}

func (m *Membership) Self() Broker {
	broker, _ := m.Get(m.self)
	return broker
}

func (m *Membership) Get(id int32) (Broker, bool) {
	for _, broker := range m.brokers {
		if broker.ID == id {
			return broker, true
		}
	}
	return Broker{}, false
}

func parseBroker(token string) (Broker, error) {
	idPart, hostPort, ok := strings.Cut(token, "@")
	if !ok || idPart == "" || hostPort == "" {
		return Broker{}, fmt.Errorf("malformed broker peer %q", token)
	}
	id, err := strconv.ParseInt(idPart, 10, 32)
	if err != nil {
		return Broker{}, fmt.Errorf("parse broker id %q: %w", idPart, err)
	}
	host, portPart, err := net.SplitHostPort(hostPort)
	if err != nil {
		return Broker{}, fmt.Errorf("parse broker address %q: %w", hostPort, err)
	}
	port, err := strconv.ParseInt(portPart, 10, 32)
	if err != nil {
		return Broker{}, fmt.Errorf("parse broker port %q: %w", portPart, err)
	}
	return Broker{ID: int32(id), Host: host, Port: int32(port)}, nil
}
