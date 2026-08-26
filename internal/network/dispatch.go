package network

import (
	"bytes"
	"io"
	"log"
	"net"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

type HandlerFunc func(header protocol.RequestHeader, body []byte) ([]byte, error)

var handlers = map[int16]HandlerFunc{}

func Dispatch(conn net.Conn, payload []byte) error {
	reader := bytes.NewReader(payload)
	dec := protocol.NewDecoder(reader)

	header, err := protocol.ReadRequestHeader(dec)
	if err != nil {
		log.Printf("failed to read request header from %s: %v", conn.RemoteAddr(), err)
		return err
	}

	body := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, body); err != nil {
		log.Printf("failed to read request body from %s: %v", conn.RemoteAddr(), err)
		return err
	}

	handler, ok := handlers[header.APIKey]
	var responseBody []byte
	if ok {
		responseBody, err = handler(header, body)
		if err != nil {
			return err
		}
	} else {
		e := protocol.NewEncoder()
		e.WriteInt16(-1)
		responseBody = e.Bytes()
	}

	e := protocol.NewEncoder()
	protocol.WriteResponseHeader(e, protocol.ResponseHeader{CorrelationID: header.CorrelationID})
	response := append(e.Bytes(), responseBody...)
	return WriteFrame(conn, response)
}
