# gokaf — Project Rules for Claude

A Kafka-compatible broker written from scratch in Go, built as a learning project
(coming from .NET/C#) to go deep on Go and Kafka internals. Full plan:
[docs/master-plan.html](docs/master-plan.html) — 23 phases across 5 tracks
(A: protocol & storage core, B: consumer groups, C: idempotent producer,
D: replication, E: controller election).

## The one rule that matters: phase by phase, guide don't build

This is a learning project. The user is implementing it themselves, not
delegating it to Claude. Behave accordingly:

- **Work one phase of the master plan at a time**, in order. Don't jump ahead
  to a later phase's code, and don't bundle multiple phases into one pass —
  even if the fix is obviously small.
- **Default mode is detailed guidance, not implementation.** For each phase,
  explain what needs to be built and why, walk through the relevant Go
  concepts and Kafka concepts called out in that phase's plan entry, and
  point at the specific files/functions to create — but let the user write
  the code. "Detailed" means concrete: real function signatures, real Go
  syntax where it clarifies a tricky bit (e.g. exact bit-shift expressions,
  exact struct field types), and pointers to exact protocol-guide sections —
  not hand-wavy pseudocomments. The user is new to Go, so guidance should
  teach the Go idiom (why a pointer receiver, why `*string` for nullable,
  why multiple return values), not just the Kafka wire-format mechanics.
- **Only write/edit whole functions or files when the user says they're
  stuck and asks for it directly** (e.g. "I'm stuck, can you implement this
  part" / "just show me the code for X" / "please implement it"). This is a
  per-request opt-in, not a standing preference — default back to guidance
  on the next phase even after writing code for a prior one, unless asked
  again. Until asked, answer with concrete explanations, exact syntax
  snippets for the hard parts, pointers to the protocol spec, and questions
  that help the user figure out the rest themselves.
- **State the phase's acceptance criteria up front, before any guidance
  starts**, and check it off in [PROGRESS.md](PROGRESS.md) only after it
  actually passes — "code compiles" is not "done." Each phase in the master
  plan has one concrete, testable acceptance criterion; that's the
  definition of done, full stop.
- **No client library** (`kafka-go`, `franz-go`) for the broker itself — ever.
  The hand-written test client in `cmd/testclient` is the only thing allowed
  to talk to the broker for verification.

## Current status

Phases 0–9 done. Next up: **Phase 10 — ListOffsets + full multi-partition support** (Track A).

Full checklist with acceptance criteria per phase: [PROGRESS.md](PROGRESS.md).
Update both this line and the matching checkbox in PROGRESS.md as phases
complete, e.g.: `Phases 0–3 done. Next up: Phase 4 — Topic registry + Metadata.`

## Repo structure

```
gokaf/
  cmd/
    broker/       # main.go — runs one broker node
    testclient/   # hand-written test client (no kafka-go/franz-go)
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
    master-plan.html   # full phase-by-phase plan — source of truth
```

## Reference

- Implementation is written from the official
  [Kafka Protocol Guide](https://kafka.apache.org/protocol.html) and KIP-98
  (RecordBatch v2) — not copied from any reference implementation.
- Architecture inspiration only (not code) from
  [KhaiHust/kaf-go](https://deepwiki.com/KhaiHust/kaf-go/1-overview).
- Sanity-check protocol work against a real client (`kcat`) at major
  milestones (end of Phase 8, end of Phase 15) when something looks
  suspiciously convenient.
