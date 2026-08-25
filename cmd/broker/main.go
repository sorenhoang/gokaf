package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("broker listening on :9092")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			logReadErr(conn, "length prefix", err)
			return
		}

		length := binary.BigEndian.Uint32(lengthBuf)

		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			logReadErr(conn, "payload", err)
			return
		}

		log.Printf("read %d bytes:\n%s", length, hex.Dump(payload))
	}
}

func logReadErr(conn net.Conn, stage string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		log.Printf("connection closed by %s while reading %s", conn.RemoteAddr(), stage)
		return
	}
	log.Printf("read error from %s while reading %s: %v", conn.RemoteAddr(), stage, err)
}
