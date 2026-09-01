package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func main() {
	dataDir := flag.String("data", "./data", "directory for partition log segments")
	flag.Parse()

	broker := &network.Broker{
		NodeID: 1,
		Host:   "localhost",
		Port:   9092,
		Topics: topic.NewRegistry(),
		Logs:   storage.NewManager(*dataDir),
		Groups: group.NewCoordinator(3 * time.Second),
	}
	defer broker.Logs.Close()
	broker.Topics.Add(topic.Topic{
		Name: "payments",
		Partitions: []topic.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})
	loadTopicsFromDataDir(broker, *dataDir)

	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("broker listening on :9092 data_dir=%s", *dataDir)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleConnection(broker, conn)
	}
}

// loadTopicsFromDataDir rebuilds the topic registry from partition directories
// left on disk, so Fetch works after a broker restart.
//
// ponytail: stopgap until topic metadata is persisted properly. It only sees
// partitions that were actually written to (a 3-partition topic produced to
// only partition 0 comes back with 1 partition), and it resurrects a topic
// that was deleted while its segments still exist on disk. Good enough for
// manual restart checks; a real metadata log replaces it.
func loadTopicsFromDataDir(broker *network.Broker, dataDir string) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		log.Printf("scan data dir %s: %v", dataDir, err)
		return
	}

	partitionsByTopic := map[string][]int32{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name, partition, ok := parseTopicPartitionDir(entry.Name())
		if !ok {
			continue
		}
		partitionsByTopic[name] = append(partitionsByTopic[name], partition)
	}

	for name, partitionIDs := range partitionsByTopic {
		slices.Sort(partitionIDs)
		partitions := make([]topic.Partition, len(partitionIDs))
		for i, partitionID := range partitionIDs {
			partitions[i] = topic.Partition{ID: partitionID, Leader: broker.NodeID, Replicas: []int32{broker.NodeID}, ISR: []int32{broker.NodeID}}
		}
		broker.Topics.Add(topic.Topic{Name: name, Partitions: partitions})
	}
}

func parseTopicPartitionDir(dir string) (string, int32, bool) {
	separator := strings.LastIndex(dir, "-")
	if separator <= 0 || separator == len(dir)-1 {
		return "", 0, false
	}
	partition, err := strconv.ParseInt(dir[separator+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}
	return dir[:separator], int32(partition), true
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
