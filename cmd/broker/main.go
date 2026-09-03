package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"path/filepath"
	"strconv"
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
	metadataLog, err := cluster.OpenMetadataLog(filepath.Join(*dataDir, "__cluster_metadata-0"))
	if err != nil {
		log.Fatal(err)
	}
	broker.MetadataLog = metadataLog
	defer metadataLog.Close()
	monitorContext, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()
	monitor := cluster.NewLivenessMonitor(membership, nodeID, *pingInterval, 3, broker.OnPeerDown, broker.OnPeerUp)
	broker.IsPeerAlive = monitor.Alive
	broker.ControllerID = monitor.ControllerID
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
	metadataRecords, err := metadataLog.ReadFrom(0)
	if err != nil {
		log.Fatal(err)
	}
	for _, record := range metadataRecords {
		broker.ApplyMetadataRecord(record)
	}
	for _, t := range broker.Topics.All() {
		broker.Replication.StartFollowing(t)
	}
	metadataFollower := cluster.NewMetadataFollower(metadataLog, membership, nodeID, monitor.ControllerID, broker.ApplyMetadataRecord, 200*time.Millisecond)
	go metadataFollower.Run(monitorContext)

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
