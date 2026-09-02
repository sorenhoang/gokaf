package cluster

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

type BrokerClient struct {
	target Broker
}

func NewBrokerClient(target Broker) *BrokerClient {
	return &BrokerClient{target: target}
}

func (c *BrokerClient) Send(header protocol.RequestHeader, body []byte) ([]byte, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(c.target.Host, strconv.Itoa(int(c.target.Port))))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	request := append(e.Bytes(), body...)
	if err := protocol.WriteFrame(conn, request); err != nil {
		return nil, err
	}

	respPayload, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		return nil, err
	}
	if respHeader.CorrelationID != header.CorrelationID {
		return nil, fmt.Errorf("correlation id: got %d, want %d", respHeader.CorrelationID, header.CorrelationID)
	}
	return respPayload[4:], nil
}
