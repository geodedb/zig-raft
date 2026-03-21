# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A standalone Raft consensus library in Zig 0.15.1+. Implements the full Raft paper (Ongaro & Ousterhout, 2014) plus pre-vote (§9.6), joint consensus membership changes (§6), snapshotting/log compaction (§7), and chunked snapshot transfer. Extracted from the Geode distributed database for general reuse.

The library depends on `spice` (fork/join parallelism, local path dependency at `../spice`).

## Build & Test Commands

```bash
zig build                                    # build the library
zig build test --summary all                 # unit tests (in-module tests)
zig build test-integration --summary all     # integration tests (tests/integration.zig)
zig build test-scenarios --summary all       # scenario tests (election, snapshots, persistence, file_storage)
zig build test-chaos --summary all           # chaos tests (optional: -Dseed=N for deterministic replay)
zig build check                              # fast compilation check, no codegen
zig fmt src/ tests/ build.zig                # format all source

make dev     # fmt + check + test-unit (quick iteration)
make test    # unit + integration tests
make ci      # clean + build + all tests
```

## Architecture

### Ready-State Pattern

The library is consumer-driven, not event-loop-driven. The consumer calls `tick()`, `step()`, and `propose()` on `RaftNode`; the library produces a `Ready` struct each round describing what to persist, send, and apply. The Raft core is a pure deterministic state machine — it never does I/O directly.

Consumer loop: `tick()` -> `step(from, msg)` -> `propose(data)` -> `ready()` -> persist/send/apply -> `advance()`

### Pluggable Interfaces (vtable pattern)

Both `Storage` and `Transport` are runtime vtable interfaces (`*anyopaque` + `*const VTable`). This is the standard Zig pattern for dynamic dispatch without generics.

- **Storage** (`src/storage.zig`): `InMemoryStorage` (tests/ephemeral) and `FileStorage` (`src/file_storage.zig`, crash-safe with CRC32 + fdatasync + atomic rename).
- **Transport** (`src/transport.zig`): `InProcessTransport` (deterministic message queues for testing). `send()` is infallible (`void`) — implementations silently drop on failure; Raft retries via heartbeat.

### Module Layout (src/)

| Module | Responsibility |
|---|---|
| `root.zig` | Public API surface, re-exports key types |
| `node.zig` | `RaftNode` — the main state machine, all RPC handlers |
| `state.zig` | `Role`, `HardState`, `SoftState` |
| `rpc.zig` | Wire types: `Entry`, `Message` (tagged union), `Snapshot`, `ClusterConfig` |
| `log.zig` | `RaftLog` — in-memory log with unstable/stable split, backed by Storage |
| `election.zig` | `ElectionState` — timers, vote tracking, randomized timeouts |
| `replication.zig` | `ReplicationState` — per-peer progress tracking, AppendEntries construction |
| `membership.zig` | `MembershipState` — joint consensus config changes |
| `quorum.zig` | Quorum math (majority calculation, joint quorum) |
| `snapshot.zig` | `SnapshotState` — chunked transfer, local snapshot creation |
| `ready.zig` | `ReadyState` / `Ready` — collects what the consumer must process |
| `storage.zig` | `Storage` vtable interface + `InMemoryStorage` |
| `file_storage.zig` | `FileStorage` — persistent, crash-safe storage |
| `transport.zig` | `Transport` vtable interface + `InProcessTransport` |

### Test Infrastructure (tests/)

| File | Purpose |
|---|---|
| `harness.zig` | `Network` — manages multiple RaftNodes with deterministic message routing |
| `checker.zig` | `Checker` — validates Raft safety invariants (Election Safety, Log Matching, Leader Completeness, State Machine Safety) |
| `integration.zig` | Multi-node integration tests using the harness |
| `chaos.zig` | Randomized fault injection (deterministic replay via `-Dseed=N`) |
| `scenarios/` | Targeted scenario tests: election, snapshots, persistence, file_storage, membership, replication |

## Key Design Constraints

- Commands are opaque `[]const u8` — the library never inspects entry data
- All state transitions are strictly sequential; `spice` parallelism is used only for read-only work (building AppendEntries payloads, broadcasting votes, checksum computation)
- Pre-vote is enabled by default (`Config.pre_vote = true`)
- Memory ownership: `RaftLog.append()` and `InMemoryStorage.appendEntriesImpl()` duplicate entry data; callers of `log.slice()` and `storage.entries()` must free the returned slice
