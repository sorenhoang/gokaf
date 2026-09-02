package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/sorenhoang/gokaf/internal/assignor"
	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func main() {
	mode := flag.String("mode", "full", "test mode: full, produce-fetch, fetch-only, multi-partition, metadata-cluster, find-coordinator, consumer-group, consumer-group-rebalance, consumer-group-roundrobin, offset-commit-fetch, offset-commit, offset-fetch, idempotent-producer")
	addr := flag.String("addr", "localhost:9092", "broker address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	checkUnknownAPI(conn)
	checkApiVersions(conn)
	switch *mode {
	case "full":
		checkCreateTopics(conn, 44, 0)
		checkMetadataTopic(conn, "orders", true, 3)
		for i := 0; i < 10; i++ {
			checkProduce(conn, fmt.Sprintf("msg-%d", i), int32(48+i), int64(i))
		}
		checkFetch(conn)
		checkCreateTopicsDuplicate(conn)
		checkDeleteTopics(conn, "orders", 0)
		checkMetadataTopic(conn, "orders", false, 0)
		checkDeleteTopics(conn, "ghost", 3)
	case "produce-fetch":
		checkCreateTopics(conn, 44, 0)
		checkMetadataTopic(conn, "orders", true, 3)
		for i := 0; i < 10; i++ {
			checkProduce(conn, fmt.Sprintf("msg-%d", i), int32(48+i), int64(i))
		}
		checkFetch(conn)
	case "fetch-only":
		checkMetadataTopic(conn, "orders", true, 1)
		checkFetch(conn)
	case "multi-partition":
		checkMultiPartition(conn)
	case "metadata-cluster":
		checkMetadataCluster(conn)
	case "find-coordinator":
		checkFindCoordinator(conn, "group-a", 100)
		checkFindCoordinator(conn, "anything", 101)
	case "consumer-group":
		checkConsumerGroup(conn)
	case "consumer-group-rebalance":
		checkConsumerGroupRebalance(conn)
	case "consumer-group-roundrobin":
		checkConsumerGroupRoundRobin(conn)
	case "offset-commit-fetch":
		checkOffsetCommit(conn, 160, "offset-group", "offset-events", 0, 42)
		checkOffsetFetch(conn, 161, "offset-group", "offset-events", 0, 42)
	case "offset-commit":
		checkOffsetCommit(conn, 160, "offset-group", "offset-events", 0, 42)
	case "offset-fetch":
		checkOffsetFetch(conn, 161, "offset-group", "offset-events", 0, 42)
	case "idempotent-producer":
		checkIdempotentProducer(conn)
	default:
		log.Fatal("unknown mode: " + *mode)
	}
}

func checkUnknownAPI(conn net.Conn) {
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

func checkApiVersions(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        18,
		APIVersion:    0,
		CorrelationID: 43,
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
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(err)
	}
	apiCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(err)
	}

	foundListOffsets := false
	foundOffsetCommit := false
	foundOffsetFetch := false
	foundApiVersions := false
	foundFindCoordinator := false
	foundJoinGroup := false
	foundHeartbeat := false
	foundLeaveGroup := false
	foundSyncGroup := false
	foundInitProducerID := false
	for i := 0; i < apiCount; i++ {
		apiKey, minVersion, maxVersion := readAPIVersionEntry(dec)
		log.Printf("api_versions entry: api_key=%d min_version=%d max_version=%d", apiKey, minVersion, maxVersion)
		if apiKey == 2 && minVersion == 1 && maxVersion == 1 {
			foundListOffsets = true
		}
		if apiKey == 8 && minVersion == 0 && maxVersion == 0 {
			foundOffsetCommit = true
		}
		if apiKey == 9 && minVersion == 0 && maxVersion == 0 {
			foundOffsetFetch = true
		}
		if apiKey == 10 && minVersion == 0 && maxVersion == 0 {
			foundFindCoordinator = true
		}
		if apiKey == 11 && minVersion == 0 && maxVersion == 0 {
			foundJoinGroup = true
		}
		if apiKey == 12 && minVersion == 0 && maxVersion == 0 {
			foundHeartbeat = true
		}
		if apiKey == 13 && minVersion == 0 && maxVersion == 0 {
			foundLeaveGroup = true
		}
		if apiKey == 14 && minVersion == 0 && maxVersion == 0 {
			foundSyncGroup = true
		}
		if apiKey == 18 && minVersion == 0 && maxVersion == 0 {
			foundApiVersions = true
		}
		if apiKey == 22 && minVersion == 0 && maxVersion == 0 {
			foundInitProducerID = true
		}
	}

	log.Printf("api_versions response: correlation_id=%d error_code=%d api_count=%d", respHeader.CorrelationID, errorCode, apiCount)
	if respHeader.CorrelationID != 43 || errorCode != 0 || !foundApiVersions || !foundListOffsets || !foundOffsetCommit || !foundOffsetFetch || !foundFindCoordinator || !foundJoinGroup || !foundHeartbeat || !foundLeaveGroup || !foundSyncGroup || !foundInitProducerID {
		log.Fatal("unexpected ApiVersions response: correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)) + " error_code=" + strconv.Itoa(int(errorCode)) + " found_api_versions=" + strconv.FormatBool(foundApiVersions) + " found_list_offsets=" + strconv.FormatBool(foundListOffsets) + " found_offset_commit=" + strconv.FormatBool(foundOffsetCommit) + " found_offset_fetch=" + strconv.FormatBool(foundOffsetFetch) + " found_find_coordinator=" + strconv.FormatBool(foundFindCoordinator) + " found_join_group=" + strconv.FormatBool(foundJoinGroup) + " found_heartbeat=" + strconv.FormatBool(foundHeartbeat) + " found_leave_group=" + strconv.FormatBool(foundLeaveGroup) + " found_sync_group=" + strconv.FormatBool(foundSyncGroup) + " found_init_producer_id=" + strconv.FormatBool(foundInitProducerID))
	}
}

func checkFindCoordinator(conn net.Conn, groupID string, correlationID int32) {
	header := protocol.RequestHeader{
		APIKey:        10,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
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
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read find_coordinator error_code: %w", err))
	}
	nodeID, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read find_coordinator node_id: %w", err))
	}
	host, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read find_coordinator host: %w", err))
	}
	port, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read find_coordinator port: %w", err))
	}

	log.Printf("find_coordinator response: group_id=%s correlation_id=%d error_code=%d node_id=%d host=%s port=%d", groupID, respHeader.CorrelationID, errorCode, nodeID, host, port)
	if respHeader.CorrelationID != correlationID || errorCode != 0 || nodeID != 1 || host != "localhost" || port != 9092 {
		log.Fatal("unexpected FindCoordinator response")
	}
}

func checkInitProducerID(conn net.Conn, correlationID int32) (int64, int16) {
	header := protocol.RequestHeader{
		APIKey:        22,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteNullableString(nil)
	e.WriteInt32(-1)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected InitProducerId correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	throttleTimeMS, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read init_producer_id throttle_time_ms: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read init_producer_id error_code: %w", err))
	}
	pid, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read init_producer_id producer_id: %w", err))
	}
	epoch, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read init_producer_id producer_epoch: %w", err))
	}

	log.Printf("init_producer_id response: throttle_time_ms=%d error_code=%d producer_id=%d producer_epoch=%d", throttleTimeMS, errorCode, pid, epoch)
	if throttleTimeMS != 0 || errorCode != 0 || pid < 0 || epoch != 0 {
		log.Fatal("unexpected InitProducerId response")
	}
	return pid, epoch
}

func checkOffsetCommit(conn net.Conn, correlationID int32, groupID string, topic string, partition int32, offset int64) {
	header := protocol.RequestHeader{
		APIKey:        8,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteArrayLen(1)
	e.WriteString(topic)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(offset)
	e.WriteNullableString(nil)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected OffsetCommit correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_commit topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected OffsetCommit topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_commit topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_commit partition count: %w", err))
	}
	gotPartition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_commit partition: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_commit error_code: %w", err))
	}
	log.Printf("offset_commit response: group=%s topic=%s partition=%d error_code=%d", groupID, topicName, gotPartition, errorCode)
	if topicName != topic || partitionCount != 1 || gotPartition != partition || errorCode != 0 {
		log.Fatal("unexpected OffsetCommit response")
	}
}

func checkOffsetFetch(conn net.Conn, correlationID int32, groupID string, topic string, partition int32, wantOffset int64) {
	header := protocol.RequestHeader{
		APIKey:        9,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteArrayLen(1)
	e.WriteString(topic)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected OffsetFetch correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected OffsetFetch topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch partition count: %w", err))
	}
	gotPartition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch partition: %w", err))
	}
	committedOffset, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch committed_offset: %w", err))
	}
	metadata, err := dec.ReadNullableString()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch metadata: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read offset_fetch error_code: %w", err))
	}
	log.Printf("offset_fetch response: group=%s topic=%s partition=%d committed_offset=%d metadata=%v error_code=%d", groupID, topicName, gotPartition, committedOffset, metadata, errorCode)
	if topicName != topic || partitionCount != 1 || gotPartition != partition || committedOffset != wantOffset || metadata != nil || errorCode != 0 {
		log.Fatal("unexpected OffsetFetch response")
	}
}

type joinGroupResult struct {
	generationID int32
	protocolName string
	leaderID     string
	memberID     string
	members      []joinGroupMember
}

type joinGroupMember struct {
	id       string
	metadata []byte
}

type syncGroupResult struct {
	memberID   string
	assignment []byte
}

type topicAssignment struct {
	topic      string
	partitions []int32
}

func checkConsumerGroup(conn net.Conn) {
	checkCreateTopic(conn, "group-events", 3, 120, 0)

	followerConn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer followerConn.Close()

	groupID := "orders-consumers"
	topicName := "group-events"
	firstCh := joinGroupAsync(conn, groupID, "client-a", 121, []string{topicName})
	secondCh := joinGroupAsync(followerConn, groupID, "client-b", 122, []string{topicName})
	first := receiveJoinGroup(firstCh)
	second := receiveJoinGroup(secondCh)

	leader, follower := first, second
	leaderConn, followerSyncConn := conn, followerConn
	if second.memberID == second.leaderID {
		leader, follower = second, first
		leaderConn, followerSyncConn = followerConn, conn
	}

	if leader.leaderID == "" || leader.leaderID != leader.memberID || follower.leaderID != leader.memberID {
		log.Fatal("unexpected JoinGroup leader/member ids")
	}
	if leader.generationID != 1 || follower.generationID != 1 || leader.protocolName != "range" || follower.protocolName != "range" {
		log.Fatal("unexpected JoinGroup generation/protocol")
	}
	if len(leader.members) != 2 || len(follower.members) != 0 {
		log.Fatal("unexpected JoinGroup members array sizes")
	}

	assignments := buildAssignments(leader.members, leader.protocolName, map[string]int32{topicName: 3})
	followerSyncCh := syncGroupAsync(followerSyncConn, groupID, follower.generationID, follower.memberID, nil, 123)
	leaderSyncCh := syncGroupAsync(leaderConn, groupID, leader.generationID, leader.memberID, assignments, 124)

	leaderSync := receiveSyncGroup(leaderSyncCh)
	followerSync := receiveSyncGroup(followerSyncCh)
	assertConsumerGroupAssignments(map[string][]byte{
		leaderSync.memberID:   leaderSync.assignment,
		followerSync.memberID: followerSync.assignment,
	}, topicName, 3)
}

func checkConsumerGroupRoundRobin(conn net.Conn) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	groupID := "roundrobin-consumers-" + suffix
	topicName := "roundrobin-events-" + suffix
	checkCreateTopic(conn, topicName, 3, 125, 0)

	followerConn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer followerConn.Close()

	firstCh := joinGroupAsyncWithProtocols(conn, groupID, "client-a", "", 30000, 126, []string{topicName}, []string{"roundrobin"})
	secondCh := joinGroupAsyncWithProtocols(followerConn, groupID, "client-b", "", 30000, 127, []string{topicName}, []string{"roundrobin"})
	first := receiveJoinGroup(firstCh)
	second := receiveJoinGroup(secondCh)

	leader, follower := first, second
	leaderConn, followerSyncConn := conn, followerConn
	if second.memberID == second.leaderID {
		leader, follower = second, first
		leaderConn, followerSyncConn = followerConn, conn
	}
	if leader.protocolName != "roundrobin" || follower.protocolName != "roundrobin" {
		log.Fatal("unexpected JoinGroup protocol for roundrobin mode")
	}

	assignments := buildAssignments(leader.members, leader.protocolName, map[string]int32{topicName: 3})
	followerSyncCh := syncGroupAsync(followerSyncConn, groupID, follower.generationID, follower.memberID, nil, 128)
	leaderSyncCh := syncGroupAsync(leaderConn, groupID, leader.generationID, leader.memberID, assignments, 129)
	leaderSync := receiveSyncGroup(leaderSyncCh)
	followerSync := receiveSyncGroup(followerSyncCh)

	got := map[string][]int32{
		leaderSync.memberID:   partitionsForAssignment(leaderSync.assignment, topicName),
		followerSync.memberID: partitionsForAssignment(followerSync.assignment, topicName),
	}
	memberIDs := []string{first.memberID, second.memberID}
	sort.Strings(memberIDs)
	want := map[string][]int32{
		memberIDs[0]: {0, 2},
		memberIDs[1]: {1},
	}
	if !equalPartitionMap(got, want) {
		log.Fatal("unexpected roundrobin assignment")
	}
	log.Printf("roundrobin assignment exact: %v", got)
}

func checkConsumerGroupRebalance(conn net.Conn) {
	deadConn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer deadConn.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	groupID := "rebalance-consumers-" + suffix
	topicName := "rebalance-events-" + suffix
	checkCreateTopic(conn, topicName, 3, 130, 0)
	sessionTimeoutMS := int32(1500)
	firstCh := joinGroupAsyncWithMember(conn, groupID, "survivor", "", sessionTimeoutMS, 131, []string{topicName})
	secondCh := joinGroupAsyncWithMember(deadConn, groupID, "crasher", "", sessionTimeoutMS, 132, []string{topicName})
	first := receiveJoinGroup(firstCh)
	second := receiveJoinGroup(secondCh)

	leader, follower := first, second
	leaderConn, followerSyncConn := conn, deadConn
	if second.memberID == second.leaderID {
		leader, follower = second, first
		leaderConn, followerSyncConn = deadConn, conn
	}
	assignments := buildAssignments(leader.members, leader.protocolName, map[string]int32{topicName: 3})
	followerSyncCh := syncGroupAsync(followerSyncConn, groupID, follower.generationID, follower.memberID, nil, 133)
	leaderSyncCh := syncGroupAsync(leaderConn, groupID, leader.generationID, leader.memberID, assignments, 134)
	leaderSync := receiveSyncGroup(leaderSyncCh)
	followerSync := receiveSyncGroup(followerSyncCh)
	assertConsumerGroupAssignments(map[string][]byte{
		leaderSync.memberID:   leaderSync.assignment,
		followerSync.memberID: followerSync.assignment,
	}, topicName, 3)

	if second.memberID == first.memberID {
		log.Fatal("unexpected duplicate member id")
	}
	survivor := first
	crashedMemberID := second.memberID
	log.Printf("simulated crash: member_id=%s stopped heartbeating", crashedMemberID)

	var heartbeatCode int16
	for attempts := 0; attempts < 10; attempts++ {
		time.Sleep(300 * time.Millisecond)
		heartbeatCode = checkHeartbeat(conn, groupID, survivor.generationID, survivor.memberID, int32(140+attempts))
		if heartbeatCode == 27 {
			break
		}
		if heartbeatCode != 0 {
			log.Fatal("unexpected Heartbeat error_code=" + strconv.Itoa(int(heartbeatCode)))
		}
	}
	if heartbeatCode != 27 {
		log.Fatal("Heartbeat did not return REBALANCE_IN_PROGRESS")
	}

	rejoined := checkJoinGroup(conn, groupID, "survivor", survivor.memberID, sessionTimeoutMS, 151, []string{topicName})
	if rejoined.generationID != 2 || rejoined.memberID != survivor.memberID || rejoined.leaderID != survivor.memberID || len(rejoined.members) != 1 {
		log.Fatal("unexpected rejoin response after rebalance")
	}
	rejoinAssignments := buildAssignments(rejoined.members, rejoined.protocolName, map[string]int32{topicName: 3})
	rejoinedSync := checkSyncGroup(conn, groupID, rejoined.generationID, rejoined.memberID, rejoinAssignments, 152)
	assertConsumerGroupAssignments(map[string][]byte{rejoinedSync.memberID: rejoinedSync.assignment}, topicName, 3)
	if code := checkLeaveGroup(conn, groupID, rejoined.memberID, 153); code != 0 {
		log.Fatal("unexpected LeaveGroup error_code=" + strconv.Itoa(int(code)))
	}
}

func checkHeartbeat(conn net.Conn, groupID string, generationID int32, memberID string, correlationID int32) int16 {
	header := protocol.RequestHeader{
		APIKey:        12,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}
	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteInt32(generationID)
	e.WriteString(memberID)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected Heartbeat correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read heartbeat error_code: %w", err))
	}
	log.Printf("heartbeat response: member_id=%s error_code=%d", memberID, errorCode)
	return errorCode
}

func checkLeaveGroup(conn net.Conn, groupID string, memberID string, correlationID int32) int16 {
	header := protocol.RequestHeader{
		APIKey:        13,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}
	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteString(memberID)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected LeaveGroup correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read leave_group error_code: %w", err))
	}
	log.Printf("leave_group response: member_id=%s error_code=%d", memberID, errorCode)
	return errorCode
}

func joinGroupAsync(conn net.Conn, groupID string, clientID string, correlationID int32, topics []string) <-chan joinGroupResult {
	return joinGroupAsyncWithMember(conn, groupID, clientID, "", 30000, correlationID, topics)
}

func joinGroupAsyncWithMember(conn net.Conn, groupID string, clientID string, memberID string, sessionTimeoutMS int32, correlationID int32, topics []string) <-chan joinGroupResult {
	return joinGroupAsyncWithProtocols(conn, groupID, clientID, memberID, sessionTimeoutMS, correlationID, topics, []string{"range", "roundrobin"})
}

func joinGroupAsyncWithProtocols(conn net.Conn, groupID string, clientID string, memberID string, sessionTimeoutMS int32, correlationID int32, topics []string, protocols []string) <-chan joinGroupResult {
	ch := make(chan joinGroupResult, 1)
	go func() {
		ch <- checkJoinGroupWithProtocols(conn, groupID, clientID, memberID, sessionTimeoutMS, correlationID, topics, protocols)
	}()
	return ch
}

func checkJoinGroup(conn net.Conn, groupID string, clientID string, memberID string, sessionTimeoutMS int32, correlationID int32, topics []string) joinGroupResult {
	return checkJoinGroupWithProtocols(conn, groupID, clientID, memberID, sessionTimeoutMS, correlationID, topics, []string{"range", "roundrobin"})
}

func checkJoinGroupWithProtocols(conn net.Conn, groupID string, clientID string, memberID string, sessionTimeoutMS int32, correlationID int32, topics []string, protocols []string) joinGroupResult {
	headerClientID := clientID
	header := protocol.RequestHeader{
		APIKey:        11,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      &headerClientID,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteInt32(sessionTimeoutMS)
	e.WriteString(memberID)
	e.WriteString("consumer")
	e.WriteArrayLen(len(protocols))
	for _, protocolName := range protocols {
		e.WriteString(protocolName)
		e.WriteBytes(encodeSubscription(topics))
	}
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected JoinGroup correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group error_code: %w", err))
	}
	generationID, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group generation_id: %w", err))
	}
	protocolName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group protocol_name: %w", err))
	}
	leaderID, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group leader: %w", err))
	}
	resolvedMemberID, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group member_id: %w", err))
	}
	memberCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read join_group member count: %w", err))
	}
	if errorCode != 0 {
		log.Fatal("unexpected JoinGroup error_code=" + strconv.Itoa(int(errorCode)))
	}

	members := make([]joinGroupMember, 0, memberCount)
	for i := 0; i < memberCount; i++ {
		id, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read join_group response member_id: %w", err))
		}
		metadata, err := dec.ReadBytes()
		if err != nil {
			log.Fatal(fmt.Errorf("read join_group response metadata: %w", err))
		}
		members = append(members, joinGroupMember{id: id, metadata: metadata})
	}

	log.Printf("join_group response: client_id=%s generation_id=%d protocol=%s leader=%s member_id=%s members=%d", clientID, generationID, protocolName, leaderID, resolvedMemberID, len(members))
	return joinGroupResult{
		generationID: generationID,
		protocolName: protocolName,
		leaderID:     leaderID,
		memberID:     resolvedMemberID,
		members:      members,
	}
}

func syncGroupAsync(conn net.Conn, groupID string, generationID int32, memberID string, assignments map[string][]byte, correlationID int32) <-chan syncGroupResult {
	ch := make(chan syncGroupResult, 1)
	go func() {
		ch <- checkSyncGroup(conn, groupID, generationID, memberID, assignments, correlationID)
	}()
	return ch
}

func checkSyncGroup(conn net.Conn, groupID string, generationID int32, memberID string, assignments map[string][]byte, correlationID int32) syncGroupResult {
	header := protocol.RequestHeader{
		APIKey:        14,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteString(groupID)
	e.WriteInt32(generationID)
	e.WriteString(memberID)
	e.WriteArrayLen(len(assignments))
	for assignmentMemberID, assignment := range assignments {
		e.WriteString(assignmentMemberID)
		e.WriteBytes(assignment)
	}
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected SyncGroup correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read sync_group error_code: %w", err))
	}
	assignment, err := dec.ReadBytes()
	if err != nil {
		log.Fatal(fmt.Errorf("read sync_group assignment: %w", err))
	}
	if errorCode != 0 {
		log.Fatal("unexpected SyncGroup error_code=" + strconv.Itoa(int(errorCode)))
	}
	log.Printf("sync_group response: member_id=%s assignment_bytes=%d", memberID, len(assignment))
	return syncGroupResult{memberID: memberID, assignment: assignment}
}

func receiveJoinGroup(ch <-chan joinGroupResult) joinGroupResult {
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for JoinGroup response")
		return joinGroupResult{}
	}
}

func receiveSyncGroup(ch <-chan syncGroupResult) syncGroupResult {
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for SyncGroup response")
		return syncGroupResult{}
	}
}

func encodeSubscription(topics []string) []byte {
	e := protocol.NewEncoder()
	e.WriteInt16(0)
	e.WriteArrayLen(len(topics))
	for _, topic := range topics {
		e.WriteString(topic)
	}
	e.WriteInt32(-1)
	return e.Bytes()
}

func decodeSubscription(b []byte) []string {
	dec := protocol.NewDecoder(bytes.NewReader(b))
	if _, err := dec.ReadInt16(); err != nil {
		log.Fatal(fmt.Errorf("read subscription version: %w", err))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read subscription topic count: %w", err))
	}
	topics := make([]string, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topic, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read subscription topic: %w", err))
		}
		topics = append(topics, topic)
	}
	if _, err := dec.ReadBytes(); err != nil {
		log.Fatal(fmt.Errorf("read subscription user_data: %w", err))
	}
	return topics
}

func encodeAssignment(assignments []topicAssignment) []byte {
	e := protocol.NewEncoder()
	e.WriteInt16(0)
	e.WriteArrayLen(len(assignments))
	for _, assignment := range assignments {
		e.WriteString(assignment.topic)
		e.WriteArrayLen(len(assignment.partitions))
		for _, partition := range assignment.partitions {
			e.WriteInt32(partition)
		}
	}
	e.WriteInt32(-1)
	return e.Bytes()
}

func decodeAssignment(b []byte) []topicAssignment {
	dec := protocol.NewDecoder(bytes.NewReader(b))
	if _, err := dec.ReadInt16(); err != nil {
		log.Fatal(fmt.Errorf("read assignment version: %w", err))
	}
	assignmentCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read assignment count: %w", err))
	}
	assignments := make([]topicAssignment, 0, assignmentCount)
	for i := 0; i < assignmentCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read assignment topic: %w", err))
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			log.Fatal(fmt.Errorf("read assignment partition count: %w", err))
		}
		partitions := make([]int32, 0, partitionCount)
		for j := 0; j < partitionCount; j++ {
			partition, err := dec.ReadInt32()
			if err != nil {
				log.Fatal(fmt.Errorf("read assignment partition: %w", err))
			}
			partitions = append(partitions, partition)
		}
		assignments = append(assignments, topicAssignment{topic: topicName, partitions: partitions})
	}
	if _, err := dec.ReadBytes(); err != nil {
		log.Fatal(fmt.Errorf("read assignment user_data: %w", err))
	}
	return assignments
}

func buildAssignments(members []joinGroupMember, protocolName string, partitionCounts map[string]int32) map[string][]byte {
	subs := make([]assignor.Subscription, 0, len(members))
	for _, member := range members {
		subs = append(subs, assignor.Subscription{MemberID: member.id, Topics: decodeSubscription(member.metadata)})
	}

	var assigned map[string][]assignor.TopicPartitions
	switch protocolName {
	case "range":
		assigned = assignor.Range(subs, partitionCounts)
	case "roundrobin":
		assigned = assignor.RoundRobin(subs, partitionCounts)
	default:
		log.Fatal("unsupported assignment protocol: " + protocolName)
	}

	assignments := make(map[string][]byte, len(members))
	for _, member := range members {
		assignments[member.id] = encodeAssignment(toTopicAssignments(assigned[member.id]))
	}
	return assignments
}

func toTopicAssignments(assignments []assignor.TopicPartitions) []topicAssignment {
	converted := make([]topicAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		converted = append(converted, topicAssignment{topic: assignment.Topic, partitions: assignment.Partitions})
	}
	return converted
}

func assertConsumerGroupAssignments(assignments map[string][]byte, topicName string, partitionCount int32) {
	seen := map[int32]string{}
	for memberID, assignmentBytes := range assignments {
		decoded := decodeAssignment(assignmentBytes)
		for _, assignment := range decoded {
			if assignment.topic != topicName {
				log.Fatal("unexpected assignment topic=" + assignment.topic)
			}
			for _, partition := range assignment.partitions {
				if priorMemberID, ok := seen[partition]; ok {
					log.Fatal("partition assigned twice: partition=" + strconv.Itoa(int(partition)) + " first=" + priorMemberID + " second=" + memberID)
				}
				seen[partition] = memberID
			}
		}
	}
	for partition := int32(0); partition < partitionCount; partition++ {
		if _, ok := seen[partition]; !ok {
			log.Fatal("partition was not assigned: " + strconv.Itoa(int(partition)))
		}
	}
	log.Printf("consumer group assignment complete: topic=%s partitions=%v", topicName, seen)
}

func partitionsForAssignment(assignmentBytes []byte, topicName string) []int32 {
	var partitions []int32
	for _, assignment := range decodeAssignment(assignmentBytes) {
		if assignment.topic == topicName {
			partitions = append(partitions, assignment.partitions...)
		}
	}
	return partitions
}

func equalPartitionMap(a map[string][]int32, b map[string][]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for memberID, aPartitions := range a {
		bPartitions, ok := b[memberID]
		if !ok || len(aPartitions) != len(bPartitions) {
			return false
		}
		for i := range aPartitions {
			if aPartitions[i] != bPartitions[i] {
				return false
			}
		}
	}
	return true
}

func readAPIVersionEntry(dec *protocol.Decoder) (int16, int16, int16) {
	apiKey, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read api_key: %w", err))
	}
	minVersion, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read min_version: %w", err))
	}
	maxVersion, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read max_version: %w", err))
	}
	return apiKey, minVersion, maxVersion
}

func checkCreateTopics(conn net.Conn, correlationID int32, wantCode int16) {
	checkCreateTopic(conn, "orders", 3, correlationID, wantCode)
}

func checkCreateTopic(conn net.Conn, name string, partitions int32, correlationID int32, wantCode int16) {
	header := protocol.RequestHeader{
		APIKey:        19,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString(name)
	e.WriteInt32(partitions)
	e.WriteInt16(1)
	e.WriteArrayLen(0)
	e.WriteArrayLen(0)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), correlationID, "create_topics", name, wantCode)
}

func checkCreateTopicsDuplicate(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        19,
		APIVersion:    0,
		CorrelationID: 46,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString("orders")
	e.WriteInt32(3)
	e.WriteInt16(1)
	e.WriteArrayLen(0)
	e.WriteArrayLen(0)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), 46, "create_topics duplicate", "orders", 36)
}

func checkDeleteTopics(conn net.Conn, name string, wantCode int16) {
	header := protocol.RequestHeader{
		APIKey:        20,
		APIVersion:    0,
		CorrelationID: 47,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString(name)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), 47, "delete_topics", name, wantCode)
}

func checkProduce(conn net.Conn, value string, correlationID int32, wantBaseOffset int64) {
	checkProduceToPartition(conn, "orders", 0, value, correlationID, wantBaseOffset)
}

func checkProduceToPartition(conn net.Conn, topic string, partition int32, value string, correlationID int32, wantBaseOffset int64) {
	batch := buildRecordBatch(value, -1, -1, -1)
	baseOffset, errorCode := produceRawBatch(conn, topic, partition, batch, correlationID)
	if errorCode != 0 || baseOffset != wantBaseOffset {
		log.Fatal("unexpected Produce response")
	}
}

func produceRawBatch(conn net.Conn, topic string, partition int32, batch []byte, correlationID int32) (int64, int16) {
	header := protocol.RequestHeader{
		APIKey:        0,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteInt16(1)
	e.WriteInt32(5000)
	e.WriteArrayLen(1)
	e.WriteString(topic)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt32(int32(len(batch)))
	request := append(e.Bytes(), batch...)

	if err := network.WriteFrame(conn, request); err != nil {
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected Produce correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected Produce topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce partition count: %w", err))
	}
	if topicName != topic || partitionCount != 1 {
		log.Fatal("unexpected Produce topic response")
	}
	gotPartition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce partition: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce error_code: %w", err))
	}
	baseOffset, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce base_offset: %w", err))
	}

	log.Printf("produce response: topic=%s partition=%d error_code=%d base_offset=%d", topicName, gotPartition, errorCode, baseOffset)
	if gotPartition != partition {
		log.Fatal("unexpected Produce response")
	}
	return baseOffset, errorCode
}

func checkIdempotentProducer(conn net.Conn) {
	pid, epoch := checkInitProducerID(conn, 170)
	topicName := "idem-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	checkCreateTopic(conn, topicName, 1, 171, 0)

	batch := buildRecordBatch("dup-me", pid, epoch, 0)
	firstOffset, firstCode := produceRawBatch(conn, topicName, 0, batch, 172)
	secondOffset, secondCode := produceRawBatch(conn, topicName, 0, batch, 173)
	if firstCode != 0 || secondCode != 0 || firstOffset != secondOffset {
		log.Fatal("idempotent retry was not deduplicated")
	}

	secondBatch := buildRecordBatch("second", pid, epoch, 1)
	nextOffset, nextCode := produceRawBatch(conn, topicName, 0, secondBatch, 174)
	if nextCode != 0 || nextOffset != firstOffset+1 {
		log.Fatal("idempotent next sequence did not append at the next offset")
	}

	gapBatch := buildRecordBatch("gap", pid, epoch, 5)
	gapOffset, gapCode := produceRawBatch(conn, topicName, 0, gapBatch, 175)
	if gapCode != 45 || gapOffset != -1 {
		log.Fatal("out-of-order idempotent batch was not rejected")
	}

	checkFetchFromPartition(conn, topicName, 0, 0, 176, 2, []string{"dup-me", "second"})
}

func checkFetch(conn net.Conn) {
	want := make([]string, 10)
	for i := range want {
		want[i] = fmt.Sprintf("msg-%d", i)
	}
	checkFetchFromPartition(conn, "orders", 0, 0, 58, 10, want)
}

func checkFetchFromPartition(conn net.Conn, topic string, partition int32, offset int64, correlationID int32, wantHighWatermark int64, wantValues []string) {
	header := protocol.RequestHeader{
		APIKey:        1,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteInt32(-1)
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteArrayLen(1)
	e.WriteString(topic)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(offset)
	e.WriteInt32(1 << 20)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected Fetch correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected Fetch topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch partition count: %w", err))
	}
	if topicName != topic || partitionCount != 1 {
		log.Fatal("unexpected Fetch topic response")
	}
	gotPartition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch partition: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch error_code: %w", err))
	}
	highWatermark, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch high_watermark: %w", err))
	}
	recordSet, err := dec.ReadBytes()
	if err != nil {
		log.Fatal(fmt.Errorf("read fetch records: %w", err))
	}
	values := decodeRecordSet(recordSet)
	log.Printf("fetch response: topic=%s partition=%d error_code=%d high_watermark=%d values=%v", topicName, gotPartition, errorCode, highWatermark, values)
	if gotPartition != partition || errorCode != 0 || highWatermark != wantHighWatermark {
		log.Fatal("unexpected Fetch partition response")
	}
	if len(values) != len(wantValues) {
		log.Fatal("unexpected Fetch value count=" + strconv.Itoa(len(values)))
	}
	for i, value := range values {
		want := wantValues[i]
		if value != want {
			log.Fatal("unexpected Fetch value at " + strconv.Itoa(i) + ": got " + value + " want " + want)
		}
	}
}

func checkListOffsets(conn net.Conn, topic string, partition int32, timestamp int64, correlationID int32, wantOffset int64) int64 {
	header := protocol.RequestHeader{
		APIKey:        2,
		APIVersion:    1,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteInt32(-1)
	e.WriteArrayLen(1)
	e.WriteString(topic)
	e.WriteArrayLen(1)
	e.WriteInt32(partition)
	e.WriteInt64(timestamp)
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected ListOffsets correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected ListOffsets topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets partition count: %w", err))
	}
	if topicName != topic || partitionCount != 1 {
		log.Fatal("unexpected ListOffsets topic response")
	}
	gotPartition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets partition: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets error_code: %w", err))
	}
	gotTimestamp, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets timestamp: %w", err))
	}
	offset, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read list offsets offset: %w", err))
	}
	log.Printf("list_offsets response: topic=%s partition=%d error_code=%d timestamp=%d offset=%d", topicName, gotPartition, errorCode, gotTimestamp, offset)
	if gotPartition != partition || errorCode != 0 || gotTimestamp != -1 || offset != wantOffset {
		log.Fatal("unexpected ListOffsets response")
	}
	return offset
}

func checkMultiPartition(conn net.Conn) {
	checkCreateTopic(conn, "events", 3, 80, 0)
	checkProduceToPartition(conn, "events", 0, "a", 81, 0)
	checkProduceToPartition(conn, "events", 0, "b", 82, 1)
	checkProduceToPartition(conn, "events", 1, "c", 83, 0)
	checkProduceToPartition(conn, "events", 2, "d", 84, 0)
	checkProduceToPartition(conn, "events", 2, "e", 85, 1)
	checkProduceToPartition(conn, "events", 2, "f", 86, 2)

	checkListOffsets(conn, "events", 1, -1, 87, 1)
	checkListOffsets(conn, "events", 2, -1, 88, 3)
	checkListOffsets(conn, "events", 0, -2, 89, 0)
	p2Start := checkListOffsets(conn, "events", 2, -2, 90, 0)

	checkFetchFromPartition(conn, "events", 2, p2Start, 91, 3, []string{"d", "e", "f"})
	checkFetchFromPartition(conn, "events", 1, 0, 92, 1, []string{"c"})
}

func writeAndAssertTopicResult(conn net.Conn, request []byte, correlationID int32, label string, wantName string, wantCode int16) {
	if err := network.WriteFrame(conn, request); err != nil {
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
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected " + label + " correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	resultCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s result count: %w", label, err))
	}
	if resultCount != 1 {
		log.Fatal("unexpected " + label + " result count=" + strconv.Itoa(resultCount))
	}
	name, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s topic name: %w", label, err))
	}
	code, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s error_code: %w", label, err))
	}
	log.Printf("%s response: name=%s error_code=%d", label, name, code)
	if name != wantName || code != wantCode {
		log.Fatal("unexpected " + label + " response")
	}
}

func buildRecordBatch(value string, producerID int64, producerEpoch int16, baseSequence int32) []byte {
	record := buildRecord(value)
	batch := make([]byte, 61, 61+len(record))

	binary.BigEndian.PutUint64(batch[0:8], 0)
	putInt32(batch[12:16], -1)
	batch[16] = 2
	putInt16(batch[21:23], 0)
	putInt32(batch[23:27], 0)
	putInt64(batch[27:35], 1700000000000)
	putInt64(batch[35:43], 1700000000000)
	putInt64(batch[43:51], producerID)
	putInt16(batch[51:53], producerEpoch)
	putInt32(batch[53:57], baseSequence)
	putInt32(batch[57:61], 1)
	batch = append(batch, record...)

	putInt32(batch[8:12], int32(len(batch)-12))
	crc := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], crc)
	return batch
}

func buildRecord(value string) []byte {
	body := protocol.NewEncoder()
	body.WriteInt8(0)
	body.WriteVarint(0)
	body.WriteVarint(0)
	body.WriteVarint(-1)
	body.WriteVarint(int32(len(value)))
	recordBody := append(body.Bytes(), []byte(value)...)
	trailer := protocol.NewEncoder()
	trailer.WriteVarint(0)
	recordBody = append(recordBody, trailer.Bytes()...)

	record := protocol.NewEncoder()
	record.WriteVarint(int32(len(recordBody)))
	return append(record.Bytes(), recordBody...)
}

func decodeRecordSet(recordSet []byte) []string {
	var values []string
	reader := bytes.NewReader(recordSet)
	for reader.Len() > 0 {
		batchStart := int64(len(recordSet) - reader.Len())
		dec := protocol.NewDecoder(reader)
		mustReadInt64(dec, "base_offset")
		batchLength := mustReadInt32(dec, "batch_length")
		mustReadInt32(dec, "partition_leader_epoch")
		magic := mustReadInt8(dec, "magic")
		if magic != 2 {
			log.Fatal("unexpected batch magic=" + strconv.Itoa(int(magic)))
		}
		mustReadInt32(dec, "crc")
		mustReadInt16(dec, "attributes")
		mustReadInt32(dec, "last_offset_delta")
		mustReadInt64(dec, "base_timestamp")
		mustReadInt64(dec, "max_timestamp")
		mustReadInt64(dec, "producer_id")
		mustReadInt16(dec, "producer_epoch")
		mustReadInt32(dec, "base_sequence")
		recordCount := mustReadInt32(dec, "record_count")
		for i := 0; i < int(recordCount); i++ {
			values = append(values, decodeRecordValue(reader, dec))
		}
		if _, err := reader.Seek(batchStart+12+int64(batchLength), io.SeekStart); err != nil {
			log.Fatal(fmt.Errorf("seek next batch: %w", err))
		}
	}
	return values
}

func decodeRecordValue(reader *bytes.Reader, dec *protocol.Decoder) string {
	mustReadVarint(dec, "record_length")
	mustReadInt8(dec, "record_attributes")
	mustReadVarint(dec, "timestamp_delta")
	mustReadVarint(dec, "offset_delta")
	keyLength := mustReadVarint(dec, "key_length")
	if keyLength > 0 {
		key := make([]byte, keyLength)
		if _, err := io.ReadFull(reader, key); err != nil {
			log.Fatal(fmt.Errorf("read key: %w", err))
		}
	}
	valueLength := mustReadVarint(dec, "value_length")
	value := make([]byte, valueLength)
	if _, err := io.ReadFull(reader, value); err != nil {
		log.Fatal(fmt.Errorf("read value: %w", err))
	}
	mustReadVarint(dec, "header_count")
	return string(value)
}

func mustReadInt8(dec *protocol.Decoder, field string) int8 {
	value, err := dec.ReadInt8()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s: %w", field, err))
	}
	return value
}

func mustReadInt16(dec *protocol.Decoder, field string) int16 {
	value, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s: %w", field, err))
	}
	return value
}

func mustReadInt32(dec *protocol.Decoder, field string) int32 {
	value, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s: %w", field, err))
	}
	return value
}

func mustReadInt64(dec *protocol.Decoder, field string) int64 {
	value, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s: %w", field, err))
	}
	return value
}

func mustReadVarint(dec *protocol.Decoder, field string) int32 {
	value, err := dec.ReadVarint()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s: %w", field, err))
	}
	return value
}

func putInt16(dst []byte, value int16) {
	binary.BigEndian.PutUint16(dst, uint16(value))
}

func putInt32(dst []byte, value int32) {
	binary.BigEndian.PutUint32(dst, uint32(value))
}

func putInt64(dst []byte, value int64) {
	binary.BigEndian.PutUint64(dst, uint64(value))
}

type metadataBroker struct {
	id   int32
	host string
	port int32
}

func checkMetadataCluster(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        3,
		APIVersion:    0,
		CorrelationID: 177,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(0)
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
	if respHeader.CorrelationID != 177 {
		log.Fatal("unexpected Metadata correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	brokers := readMetadataBrokers(dec)
	want := []metadataBroker{
		{id: 1, host: "localhost", port: 9092},
		{id: 2, host: "localhost", port: 9093},
		{id: 3, host: "localhost", port: 9094},
	}
	if !equalMetadataBrokers(brokers, want) {
		log.Fatal("unexpected Metadata cluster brokers")
	}
	log.Printf("metadata cluster brokers: %v", brokers)
}

func checkMetadataTopic(conn net.Conn, wantName string, wantPresent bool, wantPartitions int) {
	header := protocol.RequestHeader{
		APIKey:        3,
		APIVersion:    0,
		CorrelationID: 45,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(0)
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
	if respHeader.CorrelationID != 45 {
		log.Fatal("unexpected Metadata correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}

	brokers := readMetadataBrokers(dec)
	if len(brokers) < 1 {
		log.Fatal("unexpected Metadata broker count=0")
	}
	log.Printf("metadata brokers: %v", brokers)

	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read topic count: %w", err))
	}

	foundTopic := false
	for i := 0; i < topicCount; i++ {
		errorCode, err := dec.ReadInt16()
		if err != nil {
			log.Fatal(fmt.Errorf("read topic error_code: %w", err))
		}
		name, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read topic name: %w", err))
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			log.Fatal(fmt.Errorf("read partition count: %w", err))
		}
		log.Printf("metadata topic: name=%s error_code=%d partition_count=%d", name, errorCode, partitionCount)

		topicHasExpectedLeaders := partitionCount == wantPartitions
		for j := 0; j < partitionCount; j++ {
			partitionErrorCode, err := dec.ReadInt16()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition error_code: %w", err))
			}
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition index: %w", err))
			}
			leaderID, err := dec.ReadInt32()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition leader_id: %w", err))
			}
			replicaCount, err := dec.ReadArrayLen()
			if err != nil {
				log.Fatal(fmt.Errorf("read replica count: %w", err))
			}
			for k := 0; k < replicaCount; k++ {
				if _, err := dec.ReadInt32(); err != nil {
					log.Fatal(fmt.Errorf("read replica node: %w", err))
				}
			}
			isrCount, err := dec.ReadArrayLen()
			if err != nil {
				log.Fatal(fmt.Errorf("read isr count: %w", err))
			}
			for k := 0; k < isrCount; k++ {
				if _, err := dec.ReadInt32(); err != nil {
					log.Fatal(fmt.Errorf("read isr node: %w", err))
				}
			}
			log.Printf("metadata partition: topic=%s partition=%d error_code=%d leader_id=%d replicas=%d isr=%d", name, partitionIndex, partitionErrorCode, leaderID, replicaCount, isrCount)
			if partitionErrorCode != 0 {
				topicHasExpectedLeaders = false
			}
		}

		if errorCode == 0 && name == wantName && partitionCount == wantPartitions && topicHasExpectedLeaders {
			foundTopic = true
		}
	}

	if wantPresent && !foundTopic {
		log.Fatal("Metadata response did not include expected topic " + wantName)
	}
	if !wantPresent && foundTopic {
		log.Fatal("Metadata response still includes deleted topic " + wantName)
	}
}

func readMetadataBrokers(dec *protocol.Decoder) []metadataBroker {
	brokerCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read broker count: %w", err))
	}
	brokers := make([]metadataBroker, 0, brokerCount)
	for i := 0; i < brokerCount; i++ {
		nodeID, err := dec.ReadInt32()
		if err != nil {
			log.Fatal(fmt.Errorf("read broker node_id: %w", err))
		}
		host, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read broker host: %w", err))
		}
		port, err := dec.ReadInt32()
		if err != nil {
			log.Fatal(fmt.Errorf("read broker port: %w", err))
		}
		brokers = append(brokers, metadataBroker{id: nodeID, host: host, port: port})
	}
	return brokers
}

func equalMetadataBrokers(a []metadataBroker, b []metadataBroker) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
