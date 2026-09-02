package main

import (
	"context"
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

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/offset"
	"github.com/sorenhoang/gokaf/internal/producer"
	"github.com/sorenhoang/gokaf/internal/replication"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func main() {
	id := flag.Int("id", 1, "broker id")
	port := flag.Int("port", 9092, "listen port")
	peers := flag.String("peers", "", "comma-separated id@host:port for every broker, including this one")
	replicaFetchInterval := flag.Duration("replica-fetch-interval", 200*time.Millisecond, "interval between follower replica fetches")
	pingInterval := flag.Duration("ping-interval", 500*time.Millisecond, "interval between peer liveness pings")
	dataDir := flag.String("data", "./data", "directory for partition log segments")
	flag.Parse()

	nodeID := int32(*id)
	brokerPort := int32(*port)
	membership, err := cluster.ParseMembership(*peers, nodeID, "localhost", brokerPort)
	if err != nil {
		log.Fatal(err)
	}

	broker := &network.Broker{
		NodeID:    nodeID,
		Host:      "localhost",
		Port:      brokerPort,
		Topics:    topic.NewRegistry(),
		Logs:      storage.NewManager(*dataDir),
		Groups:    group.NewCoordinator(3 * time.Second),
		Producers: producer.NewManager(),
		Cluster:   membership,
	}
	broker.Replication = replication.NewManager(nodeID, broker.Logs, membership, *replicaFetchInterval)
	defer broker.Replication.StopAll()
	monitorContext, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()
	monitor := cluster.NewLivenessMonitor(membership, nodeID, *pingInterval, 3, broker.OnPeerDown, broker.OnPeerUp)
	broker.IsPeerAlive = monitor.Alive
	go monitor.Run(monitorContext)
	offsetLog, err := broker.Logs.Log("__consumer_offsets", 0)
	if err != nil {
		log.Fatal(err)
	}
	broker.Offsets, err = offset.NewStore(offsetLog)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Logs.Close()
	loadTopicsFromDataDir(broker, *dataDir)
	for _, t := range broker.Topics.All() {
		broker.Replication.StartFollowing(t)
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("broker listening on :%d data_dir=%s", *port, *dataDir)

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
// only partition 0 comes back with 1 partition), resurrects a topic that was
// deleted while its segments still exist on disk, and rebuilds every partition
// as a single self-led replica — so after a restart a follower stops
// replicating and thinks it owns its local copy. Good enough for manual
// restart checks; the Phase 23 metadata log replaces it.
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
	if strings.HasPrefix(dir[:separator], "__") {
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
