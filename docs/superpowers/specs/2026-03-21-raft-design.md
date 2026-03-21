# Raft Consensus Library — Design Spec

**Date:** 2026-03-21
**Status:** Approved
**Source reference:** `/home/benji/src/gl/devnw/codepros/geode/geode/src/distributed/`

---

## 1. Overview

A generalized, standalone Raft consensus library extracted from the Geode distributed database and generalized for reuse by any consumer. Written in Zig 0.15.1+. Zero external dependencies except `spice` (fork/join parallelism).

The library implements the full Raft paper (Ongaro & Ousterhout, 2014) plus:
- Pre-vote optimization (§9.6)
- Joint consensus membership changes (§6)
- Snapshotting and log compaction (§7)
- Chunked snapshot transfer via `InstallSnapshot` RPC

---

## 2. Core Design Decisions

### 2.1 Opaque Command Bytes (Ready-State Pattern)

The library is agnostic to what is being replicated. Commands are `[]const u8`. The consumer owns serialization and deserialization. The library never inspects entry data.

The API follows the **ready-state pattern** (as used by etcd/raft): the consumer drives the node by calling `tick()`, `step()`, and `propose()`; the library produces a `Ready` struct each round that tells the consumer exactly what to persist, send, and apply — in that order.

This separation ensures:
- The Raft core is a pure, deterministic function of its inputs
- Every layer is independently testable with simple byte slices
- The algorithm maps 1:1 to the TLA+ spec

### 2.2 Pluggable Storage (Interface + Two Implementations)

`Storage` is a runtime vtable interface. The library ships two concrete implementations:
- `InMemoryStorage` — for tests and ephemeral nodes
- `FileStorage` — append-only log file with separate index, atomic writes

Consumers can implement their own backend (embedded DB, cloud object store, etc.).

### 2.3 Pluggable Transport (Interface + In-Process Implementation)

`Transport` is a runtime vtable interface. The library ships one concrete implementation:
- `InProcessTransport` — channel-based, for single-machine tests and simulations

Consumers wire up real networking (TCP, QUIC, gRPC) themselves.

### 2.4 Spice Parallelism (Opt-In, Safety-Preserving)

`spice` (fork/join, heartbeat scheduling) is used only for work that reads but never mutates Raft state:
- Building `AppendEntries` payloads for N peers in parallel (each peer has independent `next_index`)
- Broadcasting `RequestVote` to all peers during election
- Snapshot chunk checksum computation

All state transitions, vote decisions, commit index advancement, and entry application remain strictly sequential.

`tick()` and `step()` accept an optional `*spice.Task`. The consumer owns the `ThreadPool` and calls `pool.call()` when parallel message construction is desired. Sequential operation (passing `null`) is always correct.

### 2.5 Pre-Vote Enabled by Default

Before incrementing its term and starting a real election, a candidate runs a pre-vote round to confirm it can win. This prevents a partitioned node from disrupting a stable cluster with a stale term bump on rejoin.

---

## 3. Architecture & Data Flow

```
Consumer
  │
  ├── propose(data: []const u8)         # submit command (leader only queues; others return error)
  ├── step(msg: Message, task: ?*spice.Task)   # deliver inbound RPC
  ├── tick(task: ?*spice.Task)          # advance logical clock
  │
  └── ready() → Ready                   # drain after each tick/step/propose
        │
        ├── hard_state: ?HardState      # 1. PERSIST FIRST (write-ahead guarantee)
        ├── messages: []const Message   # 2. SEND to peers (parallel w/ spice if task != null)
        ├── snapshot: ?Snapshot         # 3. APPLY snapshot to state machine (if present)
        └── committed_entries: []const Entry  # 4. APPLY to state machine, in order
              │
              └── advance()            # 5. tell node you've processed this Ready
```

**Invariant:** The consumer MUST persist `hard_state` before sending any messages. This is the write-ahead guarantee that makes Raft durable across crashes.

---

## 4. Module Breakdown

```
src/
  root.zig         # public API re-exports: RaftNode, Config, Ready, Entry, Message,
                   #   Storage, Transport, Snapshot, HardState, Role
  node.zig         # RaftNode — orchestrates all modules, produces Ready
                   #   accepts *spice.Task for parallel peer message construction
  log.zig          # RaftLog — append, truncate at conflict, compaction, index arithmetic
  state.zig        # Role enum (follower/candidate/leader), HardState, SoftState
  election.zig     # pre-vote + real vote: timeout, vote grant/deny, quorum counting
  replication.zig  # AppendEntries: build per-peer payloads, track next_index/match_index,
                   #   commit index majority advancement
  snapshot.zig     # InstallSnapshot: chunked send/receive, CRC32 verification,
                   #   log compaction trigger, consumer snapshot callbacks
  membership.zig   # joint consensus: ConfigEntry, phase transitions (joint→new_only),
                   #   voter set arithmetic, safe single-server change as special case
  rpc.zig          # Message tagged union: RequestVote | RequestVoteResponse |
                   #   AppendEntries | AppendEntriesResponse |
                   #   InstallSnapshot | InstallSnapshotResponse
  ready.zig        # Ready struct, advance() handshake, pending state between rounds
  storage.zig      # Storage vtable interface + InMemoryStorage + FileStorage
  transport.zig    # Transport vtable interface + InProcessTransport
  quorum.zig       # quorum math: simple majority + joint quorum
                   #   (old config ∩ new config both independently need majority)
```

All files target ≤400 lines. No file exceeds 800 lines.

---

## 5. Core Data Types

```zig
// state.zig
pub const Role = enum { follower, candidate, leader };

pub const HardState = struct {
    current_term: u64,
    voted_for: ?u64,   // null = not voted this term
    commit_index: u64,
};

// rpc.zig
pub const EntryKind = enum { normal, config };

pub const Entry = struct {
    index: u64,
    term: u64,
    kind: EntryKind,
    data: []const u8,  // opaque — consumer owns serialization
};

pub const SnapshotMetadata = struct {
    index: u64,
    term: u64,
    config: ClusterConfig,  // membership at snapshot point
};

pub const Snapshot = struct {
    metadata: SnapshotMetadata,
    data: []const u8,
};

// ready.zig
pub const Ready = struct {
    hard_state: ?HardState,
    snapshot: ?Snapshot,
    committed_entries: []const Entry,
    messages: []const Message,
};

// node.zig
pub const Config = struct {
    id: u64,
    peers: []const u64,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    max_entries_per_msg: u32 = 64,
    snapshot_chunk_size: usize = 64 * 1024,
    pre_vote: bool = true,
};

// storage.zig
pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        saveHardState: *const fn (*anyopaque, HardState) anyerror!void,
        appendEntries: *const fn (*anyopaque, []const Entry) anyerror!void,
        entries:       *const fn (*anyopaque, lo: u64, hi: u64) anyerror![]Entry,
        term:          *const fn (*anyopaque, index: u64) anyerror!u64,
        lastIndex:     *const fn (*anyopaque) anyerror!u64,
        saveSnapshot:  *const fn (*anyopaque, Snapshot) anyerror!void,
        loadSnapshot:  *const fn (*anyopaque) anyerror!?Snapshot,
        compact:       *const fn (*anyopaque, index: u64) anyerror!void,
    };
};

// transport.zig
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (*anyopaque, to: u64, msg: Message) void,
    };
};
```

---

## 6. Membership Changes

Joint consensus (Raft §6): when the cluster config changes, a `config` entry is appended carrying both old and new voter sets. Until that entry is committed, quorum requires majority from **both** old and new configs independently. After commitment, a second `config` entry carrying only the new config is appended and committed. At no point can two independent leaders exist.

The `quorum.zig` module handles both simple majority and joint quorum. Membership changes are exposed to consumers as a `proposeConfigChange(new_peers: []const u64)` call on the leader.

Leader removal: if the leader is not in the new config, it steps down after the new config entry commits.

---

## 7. Snapshots

Consumer provides callbacks on `RaftNode` initialization:
```zig
snapshot_fn: *const fn (ctx: *anyopaque) anyerror![]const u8,
restore_fn:  *const fn (ctx: *anyopaque, data: []const u8) anyerror!void,
```

When log compaction is triggered (configurable threshold), the library calls `snapshot_fn` to get the state machine's current serialized state, wraps it with metadata, persists via `Storage.saveSnapshot`, then trims the log via `Storage.compact`.

When a lagging follower needs to catch up, the leader sends the snapshot in chunks via `InstallSnapshot` RPC (64KB default chunk size). The follower assembles chunks, verifies CRC32, then delivers the complete `Snapshot` in the `Ready.snapshot` field for the consumer to apply via `restore_fn`.

---

## 8. Testing Strategy

### Tier 1 — Unit Tests (embedded per module)

Each module tested in isolation against the paper:
- `log.zig`: append, truncate at conflict, compaction, index arithmetic
- `election.zig`: pre-vote, vote grant/deny (term check, log up-to-date check), split vote
- `replication.zig`: `next_index` backtracking, `match_index` advancement, commit majority
- `quorum.zig`: simple majority, joint quorum correctness
- `membership.zig`: joint config transitions, voter set arithmetic

### Tier 2 — Scenario Tests (`tests/`)

Uses `InProcessTransport` + `InMemoryStorage` + `Network` simulator supporting:
- Message drop (by rate or explicit)
- Message delay (reorder delivery by N ticks)
- Bidirectional partitions between node subsets
- Node pause/resume

Key scenarios:
```
election/
  basic_leader_election
  split_vote_resolves
  pre_vote_prevents_disruption
  leader_steps_down_on_stale_term
  no_double_vote_same_term

replication/
  single_entry_commits
  pipeline_entries_commit_in_order
  follower_log_conflict_truncated
  log_backtracking_on_rejection
  no_commit_from_prior_term          # Raft §5.4.2

snapshots/
  snapshot_catches_up_laggard
  chunked_snapshot_transfer
  log_compaction_after_snapshot

membership/
  add_server_joint_consensus
  remove_server_joint_consensus
  leader_removed_steps_down
  no_double_commit_during_reconfig

persistence/
  hard_state_survives_restart
  log_survives_restart
  snapshot_survives_restart
```

### Tier 3 — Invariant Checker (runs after every tick in all tests)

```zig
pub const Checker = struct {
    // Election safety: at most one leader per term
    fn checkElectionSafety(cluster: []RaftNode) void;
    // Log matching: same index+term → identical prefix
    fn checkLogMatching(cluster: []RaftNode) void;
    // Leader completeness: every committed entry in leader's log
    fn checkLeaderCompleteness(cluster: []RaftNode) void;
    // State machine safety: same index → same entry applied on all nodes
    fn checkStateMachineSafety(cluster: []RaftNode) void;
};
```

Any violation panics immediately with full cluster state dump.

### Tier 4 — Chaos Tests (`tests/chaos/`)

`ChaosNetwork` wraps `InProcessTransport` with configurable fault injection:

```zig
pub const FaultConfig = struct {
    drop_rate: f64 = 0.0,
    delay_ticks: u32 = 0,
    reorder_rate: f64 = 0.0,
    partition: []const Partition = &.{},
    node_weights: []const NodeWeight = &.{},
};
```

`ChaosRunner` drives runs with a seeded RNG (failures are reproducible — seed printed on failure):

```zig
pub const ChaosRunner = struct {
    pub fn runTicks(self: *ChaosRunner, ticks: u64) void;
    pub fn runUntilStable(self: *ChaosRunner, stable_ticks: u64) void;
    pub fn injectPartition(self: *ChaosRunner, partition: Partition) void;
    pub fn healPartition(self: *ChaosRunner) void;
    pub fn crashNode(self: *ChaosRunner, id: u64) void;
    pub fn restartNode(self: *ChaosRunner, id: u64) void;
};
```

Chaos scenarios:
```
chaos/
  flaky_network              — 10%/30%/50% drop rates, 10k ticks, no committed entry lost
  message_reorder            — high reorder rate, log matching invariant never violated
  sustained_partition        — 3|2 split: minority can't commit, majority does; heal + catchup
  leader_isolation           — partition leader only; new leader elected; old steps down on rejoin
  rolling_restart            — cycle each node crash+restart, no committed entry lost
  cascade_failures           — kill 2 of 5 simultaneously, cluster maintains quorum
  flapping_connection        — repeatedly partition/heal same link, verify stability
  slow_follower              — 10% message delivery, follower catches up via snapshot
  election_storm             — force simultaneous elections, pre-vote breaks storm
  membership_under_chaos     — add/remove server under message loss, joint consensus correct
```

Invariant `Checker` runs after every single tick throughout all chaos scenarios.

---

## 9. Build & Dependencies

`build.zig.zon` adds spice as a path dependency:
```zig
.dependencies = .{
    .spice = .{ .path = "../spice" },
},
```

Build steps (extending existing Makefile):
- `zig build test` — unit tests
- `zig build test-integration` — scenario tests
- `zig build test-chaos` — chaos tests (accepts `-Dseed=N` for replay)
- `zig build check` — syntax only (IDE)

---

## 10. File Count Summary

| Category | Files | Estimated Lines |
|----------|-------|----------------|
| Source modules | 13 | ~3,500 |
| Unit tests (embedded) | — | ~1,000 |
| Scenario tests | ~20 files | ~2,000 |
| Chaos tests | ~10 files | ~1,000 |
| **Total** | | **~7,500** |
