package main

import (
	"errors"
	"io"
	"log"
	"net"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func main() {
	broker := &network.Broker{
		NodeID: 1,
		Host:   "localhost",
		Port:   9092,
		Topics: topic.NewRegistry(),
	}
	broker.Topics.Add(topic.Topic{
		Name: "orders",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			{ID: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})
	broker.Topics.Add(topic.Topic{
		Name: "payments",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})

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

		go handleConnection(broker, conn)
	}
}

func handleConnection(broker *network.Broker, conn net.Conn) {
	defer conn.Close()

	for {
		payload, err := network.ReadFrame(conn)
		if err != nil {
			logReadErr(conn, "frame", err)
			return
		}

		if err := broker.Dispatch(conn, payload); err != nil {
			log.Printf("dispatch error from %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func logReadErr(conn net.Conn, stage string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		log.Printf("connection closed by %s while reading %s", conn.RemoteAddr(), stage)
		return
	}
	log.Printf("read error from %s while reading %s: %v", conn.RemoteAddr(), stage, err)
}
