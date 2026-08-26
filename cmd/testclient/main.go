package main

import (
	"bytes"
	"log"
	"net"
	"strconv"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	header := protocol.RequestHeader{
		APIKey:        9999,
		APIVersion:    0,
		CorrelationID: 42,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	if err := network.WriteFrame(conn, e.Bytes()); err != nil {
		log.Fatal(err)
	}

	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	errCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("correlation_id=%d error_code=%d", respHeader.CorrelationID, errCode)
	if respHeader.CorrelationID != 42 || errCode != -1 {
		log.Fatal("unexpected response: correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)) + " error_code=" + strconv.Itoa(int(errCode)))
	}
}
