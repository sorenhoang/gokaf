# gokaf

A Kafka-compatible broker written from scratch in Go — wire protocol, storage engine,
consumer group coordination, and replication — built as a learning project to go deep
on both Go and Kafka internals.

No client library is used for the broker itself (no `kafka-go`/`franz-go`); a
hand-written test client in `cmd/testclient` talks raw bytes over TCP to validate
each phase against the official [Kafka Protocol Guide](https://kafka.apache.org/protocol.html).

See [docs/master-plan.html](docs/master-plan.html) for the full phase-by-phase plan
(Track A: protocol & storage core, Track B: consumer groups, Track C: idempotent
producer, Track D: replication, Track E: controller election).

## Status

Not started — see master plan, Phase 0.

## Layout

```
cmd/
  broker/       # broker node entrypoint
  testclient/   # hand-written test client
internal/
  protocol/     # wire format encode/decode
  network/      # TCP server, request dispatch
  storage/      # segmented append-only log + index
  topic/        # topic/partition registry
  group/        # consumer group coordinator
  producer/     # idempotent producer (PID/sequence dedup)
  replication/  # ReplicaFetcher, ISR
  cluster/      # controller election, metadata log
docs/
  master-plan.html
```

Reference architecture: [KhaiHust/kaf-go](https://deepwiki.com/KhaiHust/kaf-go/1-overview).
