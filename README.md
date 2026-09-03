# gokaf

A Kafka-compatible broker written from scratch in Go — wire protocol, segmented
storage engine, consumer-group coordination, an idempotent producer, follower
replication with ISR and `acks=all`, a bully-elected controller with a
KRaft-style metadata log, and a React admin UI over an in-broker HTTP API.

Built as a learning project (coming from .NET/C#) to go deep on both Go and
Kafka internals. **No client library** is used for the broker itself (no
`kafka-go` / `franz-go`); a hand-written test client in `cmd/testclient` talks
raw bytes over TCP, implemented from the official
[Kafka Protocol Guide](https://kafka.apache.org/protocol.html) and KIP-98.

Full phase-by-phase plan and acceptance criteria:
[docs/master-plan.html](docs/master-plan.html) · progress log:
[PROGRESS.md](PROGRESS.md).

## Status: complete

All 28 phases across 6 tracks are done, each verified against one concrete
acceptance test:

| Track | Phases | What works |
|---|---|---|
| **A — Protocol & core** | 0–10 | Size-prefixed framing, primitive codecs, request dispatch, ApiVersions, Metadata, CreateTopics/DeleteTopics, a segmented append-only log with a sparse offset index and segment rolling, Produce (v2 RecordBatch + CRC32C), Fetch, ListOffsets, multi-partition topics |
| **B — Consumer groups** | 11–15 | FindCoordinator, a JoinGroup/SyncGroup state machine (`Empty→PreparingRebalance→CompletingRebalance→Stable`) with a blocking join window, Heartbeat/LeaveGroup with session-timeout eviction, OffsetCommit/OffsetFetch over an internal `__consumer_offsets` log, and real Range / RoundRobin assignors |
| **C — Producer reliability** | 16 | InitProducerId + `(producer_id, partition) → last_sequence` dedup — a resent batch lands exactly once |
| **D — Replication** | 17–21 | Static multi-broker membership, round-robin partition-leader + replica assignment, a per-partition ReplicaFetcher loop (byte-identical follower logs), time-based ISR + high-watermark + `acks=all` that waits for the ISR, and simple lowest-live-ISR leader failover |
| **E — Controller** | 22–23 | Controller = highest live broker id (derived from ping-based liveness); a metadata log the controller writes and every broker replays on restart, so the cluster comes back with identical metadata |
| **F — Admin UI** | 24–28 | An operations layer split out of the wire handlers; an HTTP/JSON admin API; a Vite + React dashboard/console embedded via `go:embed`; consumer-group + producer inspector panels; a cluster view and a chaos panel (slow-follower, drop-pings, pause, shutdown) |

### Deliberately not implemented

This is a teaching project, not a production broker. It borrows the *shape* of
Kafka's mechanisms, not their guarantees:

- **No consensus.** The controller election and metadata log are not Raft —
  they are safe only with no network partition. A broker that was down during
  metadata writes and restarts as the highest id becomes a stale controller.
- **No auth / TLS.** Inter-broker calls ride the same listener as clients on
  unauthenticated internal API keys (1000–1002); the HTTP API sends
  `Access-Control-Allow-Origin: *`.
- **No compression, transactions, or transactional producer.** Idempotency is
  per-partition only.
- **No metadata-log compaction**, no time-index for timestamp lookups, no
  Fetch long-poll, one replica of `__consumer_offsets` and `__cluster_metadata`.
- Fetch does not enforce `NOT_LEADER` (followers serve reads) and consumer
  Fetch is not truncated at the high watermark.

Points like these are marked with `ponytail:` comments in the code.

## Running it

Requires Go 1.24+. The React UI is prebuilt and committed (embedded in the
binary); rebuild it with Node 22+ only if you change `web/`.

### Single broker

```sh
go run ./cmd/broker -data ./data -http-addr :8080
# UI + admin API on http://localhost:8080, Kafka wire protocol on :9092
```

Drive it with the hand-written client (see `-mode` for the full list):

```sh
go run ./cmd/testclient -mode full
go run ./cmd/testclient -mode consumer-group
go run ./cmd/testclient -mode idempotent-producer
```

### Three-broker cluster

```sh
PEERS=1@localhost:9092,2@localhost:9093,3@localhost:9094
go run ./cmd/broker -id 1 -port 9092 -data ./d1 -peers $PEERS -http-addr :8080 &
go run ./cmd/broker -id 2 -port 9093 -data ./d2 -peers $PEERS -http-addr :8081 &
go run ./cmd/broker -id 3 -port 9094 -data ./d3 -peers $PEERS -http-addr :8082 &
```

Each broker needs its own `-data` dir. The controller is the highest live id
(broker 3). Open `http://localhost:8080` — the UI merges every broker listed in
`web/src/brokers.ts` and flags disagreements. The Chaos tab injects faults and
shuts brokers down over HTTP so you can watch ISR shrink, a controller
re-election, and a leader failover with no terminal.

```sh
go run ./cmd/testclient -mode replica-sync    -topic r -n 10   # follower logs match
go run ./cmd/testclient -mode acks-all                          # acks=all waits, acks=1 doesn't
go run ./cmd/testclient -mode metadata-snapshot -topic r        # compare across brokers / restarts
```

### Rebuilding the UI

```sh
npm --prefix web install
npm --prefix web run build    # writes internal/httpapi/dist/, embedded on next go build
npm --prefix web run dev      # Vite dev server, proxies /api to :8080
```

### Tests

```sh
go test -race ./...
```

## Layout

```
cmd/
  broker/       # broker node entrypoint + wiring
  testclient/   # hand-written raw-TCP test client (one -mode per acceptance test)
internal/
  protocol/     # primitive + RecordBatch encode/decode, size-prefixed framing
  network/      # TCP dispatch, wire handlers, the broker operations layer
  storage/      # segmented append-only log + sparse offset index
  topic/        # topic/partition registry, replica assignment
  group/        # consumer-group coordinator state machine
  producer/     # PID manager, per-partition sequence dedup
  assignor/     # Range / RoundRobin partition assignors + assignment codec
  offset/       # __consumer_offsets store (replayed on boot)
  replication/  # ReplicaFetcher, PartitionState (ISR + high watermark)
  cluster/      # static membership, liveness monitor, metadata log + follower, BrokerClient
  faults/       # runtime chaos switches
  httpapi/      # HTTP/JSON admin API + embedded web UI
web/            # Vite + React + TypeScript admin UI
docs/
  master-plan.html
```

Architecture inspiration (not code) from
[KhaiHust/kaf-go](https://deepwiki.com/KhaiHust/kaf-go/1-overview).
