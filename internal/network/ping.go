package network

import (
	"errors"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handlePing(header protocol.RequestHeader, body []byte) ([]byte, error) {
	if b.Faults.DropPings() {
		// Refusing the ping drops the connection; the peer's read deadline
		// fires and it counts a miss, treating this broker as down.
		return nil, errors.New("ping dropped by fault injection")
	}
	return nil, nil
}
