# gokaf — Phase Progress

Tracking checklist for [docs/master-plan.html](docs/master-plan.html). Check a
phase off only once its acceptance criteria actually passes — "compiles" is
not "done." Update this file as phases complete; keep `CLAUDE.md`'s
"Current status" line in sync with the last checked phase.

## Track A — Protocol & Single-Broker Core (Phases 0–10)

- [x] **Phase 0** — TCP skeleton + spec reading
      AC: `nc -v localhost 9092` sends arbitrary bytes → broker logs the
      correct byte count and hex dump, no crash.
- [x] **Phase 1** — Binary primitive encode/decode
      AC: Unit tests round-trip encode→decode for every type and match the
      worked byte examples in the protocol guide.
- [x] **Phase 2** — Request header parsing + dispatch skeleton
      AC: Test client sends a request with an unknown api_key → broker
      replies with a correct `ResponseHeader{correlation_id}` + error code,
      no crash.
- [x] **Phase 3** — ApiVersions (key=18)
      AC: Test client sends ApiVersionsRequest → correctly parses the
      response.
- [x] **Phase 4** — Topic registry + Metadata (key=3)
      AC: `MetadataRequest(topics=nil)` returns the correct
      topic/partition/leader list.
- [x] **Phase 5** — CreateTopics / DeleteTopics (key=19/20)
      AC: Create topic "orders" with 3 partitions → Metadata reflects it
      correctly.
- [x] **Phase 6** — Storage engine — segmented log
      AC: Append 1000 fake records, read back correctly at arbitrary
      offsets. Restart the process (close/reopen the file) → data survives.
- [x] **Phase 7** — Produce API (key=0)
      AC: Test client hand-encodes a valid RecordBatch → sends Produce →
      receives the correct base offset. On-disk bytes match the worked
      examples in KIP-98.
- [x] **Phase 8** — Fetch API (key=1)
      AC: Produce 10 messages → Fetch from offset 0 → receive all 10,
      correct order, correct content.
- [x] **Phase 9** — Offset index + segment rolling
      AC: Generate enough data to produce 2+ segments; Fetch from a later
      segment returns correct data. Benchmark shows lookup is meaningfully
      faster than the Phase 6 linear scan.
- [x] **Phase 10** — ListOffsets (key=2) + full multi-partition support
      AC: Test client uses ListOffsets to compute where to start fetching
      from, like a real consumer would, verified on a multi-partition topic.

## Track B — Consumer Group (Phases 11–15)

- [x] **Phase 11** — FindCoordinator (key=10)
      AC: Correct broker info returned for any group_id.
- [x] **Phase 12** — JoinGroup / SyncGroup (key=11/14) + state machine
      AC: 2–3 simulated consumers in the same group → one is elected
      leader → SyncGroup assigns partitions with no overlap and no gaps.
- [x] **Phase 13** — Heartbeat / LeaveGroup (key=12/13) + session timeout
      AC: Simulate one member crashing (stop sending heartbeats) → after
      the timeout, the coordinator rebalances and the remaining members
      pick up its partitions.
- [x] **Phase 14** — OffsetCommit / OffsetFetch (key=8/9) + internal offsets log
      AC: Commit an offset, fully restart the broker (kill + start),
      OffsetFetch still returns the committed offset.
- [x] **Phase 15** — Real Range / RoundRobin assignors
      AC: Pure unit test of the assignor (no network): given N partitions
      and M members, output matches the worked examples in the Kafka docs.

## Track C — Producer Reliability (Phase 16)

- [x] **Phase 16** — InitProducerId (key=22) + idempotent producer
      AC: Test client deliberately resends the same batch twice (simulating
      a retry after a timeout) → the log contains exactly one copy, not two.

## Track D — Multi-Broker & Replication (Phases 17–21)

- [ ] **Phase 17** — Multi-broker bootstrap
      AC: 3 brokers started together → each broker's Metadata response
      correctly lists all 3 (id, host, port).
- [ ] **Phase 18** — Partition leader assignment across brokers
      AC: Create a topic with 6 partitions on a 3-broker cluster → leaders
      are evenly spread, 2 partitions per broker.
- [ ] **Phase 19** — ReplicaFetcher — follower replication
      AC: Produce to the leader → within a few seconds, the follower
      brokers' log files have matching checksums.
- [ ] **Phase 20** — ISR tracking + high-watermark + acks=all
      AC: Simulate a slow follower (delay its fetch loop) →
      Produce(acks=all) blocks/waits correctly, while Produce(acks=1)
      returns immediately without waiting.
- [ ] **Phase 21** — Simple leader failover
      AC: Kill the leader process mid-Produce → the producer (following the
      NotLeaderForPartition error) switches to the new leader without
      losing any data that was already in the ISR at crash time.

## Track E — Controller / Cluster Metadata (Phases 22–23)

- [ ] **Phase 22** — Controller election
      AC: Kill the current controller → the cluster elects exactly one new
      controller within a few seconds, with no split-brain, under the
      simplifying assumption that there's no network partition in the test.
- [ ] **Phase 23** — KRaft-style metadata log (optional)
      AC: Restart the entire cluster → every broker replays the log and
      ends up with identical metadata.

## Track F — Kafka UI (Phases 24–28)

React state-inspector + test console over an HTTP/JSON admin API built into
the broker. Built last, against a finished broker. Full spec + implementation
plan to be written when the track starts.

- [ ] **Phase 24** — Broker operations layer + HTTP skeleton
      AC: `curl localhost:8080/api/v1/topics` returns the correct
      topic/partition/offset list as JSON; every existing Kafka wire test
      still passes unchanged.
- [ ] **Phase 25** — Shared RecordBatch codec + produce/fetch API
      AC: Produce a keyed message via `curl`, fetch it back via `curl` →
      decoded key/value match, on-disk bytes identical to a wire Produce.
- [ ] **Phase 26** — React dashboard + console
      AC: `npm run build && go run ./cmd/broker`, open `localhost:8080`,
      create a topic → produce → browse it back, entirely from the UI.
- [ ] **Phase 27** — Consumer-group + producer panels
      AC: Run a simulated consumer group via testclient → UI shows members,
      assignments, lag updating. Reset a group offset from the UI → it
      re-consumes from there.
- [ ] **Phase 28** — Cluster view + failure injection
      AC: From the UI only — slow a follower and watch it leave the ISR;
      kill the controller and watch a new one elected; kill a leader
      mid-produce and watch failover complete. No terminal involved.
