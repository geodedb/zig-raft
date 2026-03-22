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

**Out of scope (v1):** Linearizable read-only queries (ReadIndex / lease reads). Consumers requiring linearizable reads must route them as normal log entries. A `readIndex()` API may be added in a future version.

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
- `FileStorage` — see §2.2.1 for crash-recovery protocol

Consumers can implement their own backend (embedded DB, cloud object store, etc.).

#### 2.2.1 FileStorage Crash-Recovery Protocol

- **Record format:** Each log entry is written as: `[4-byte little-endian length][protobuf-encoded Entry][4-byte CRC32 of encoded bytes]`
- **Torn write detection:** On open, scan forward until CRC mismatch or EOF. Truncate any partial trailing record (`ftruncate` to last valid offset). This means a single-entry partial write is safely discarded.
- **Index file:** A parallel `.idx` file stores `[8-byte file offset]` per entry for O(1) random access. Rebuilt from the log file if missing or corrupt.
- **Atomic writes:** `appendEntries` writes records to a temp file, calls `fdatasync`, then `rename`-replaces the log segment. On Linux `rename` is atomic within the same filesystem.
- **`compact(index)`:** Rewrites the log from `index + 1` forward into a new file, calls `fdatasync`, then `rename`-replaces atomically. The old snapshot is only deleted after the new one is confirmed written.

### 2.3 Pluggable Transport (Interface + In-Process Implementation)

`Transport` is a runtime vtable interface. The library ships one concrete implementation:
- `InProcessTransport` — channel-based, for single-machine tests and simulations

Consumers wire up real networking (TCP, QUIC, gRPC) themselves.

**Transport contract:** `send()` is infallible (`void` return). Transport implementations MUST silently drop messages on failure — Raft's heartbeat mechanism provides retry. The transport is responsible for its own internal error logging. This matches Raft's design assumption that the network is unreliable.

### 2.4 Spice Parallelism (Opt-In, Safety-Preserving)

`spice` (fork/join, heartbeat scheduling) is used only for work that reads but never mutates Raft state:
- Building `AppendEntries` payloads for N peers in parallel (each peer has independent `next_index`)
- Broadcasting `RequestVote` to all peers during election
- Snapshot chunk checksum computation

All state transitions, vote decisions, commit index advancement, and entry application remain strictly sequential.

`tick()` and `step()` accept an optional `*spice.Task`. The consumer owns the `ThreadPool` and calls `pool.call()` when parallel message construction is desired. Sequential operation (passing `null`) is always correct.

### 2.5 Pre-Vote Enabled by Default

Before incrementing its term and starting a real election, a candidate runs a pre-vote round to confirm it can win. During pre-vote:
- `current_term` and `voted_for` are NOT modified on either node
- Pre-vote and real vote messages are distinguished by a `pre_vote: bool` field on `RequestVote` / `RequestVoteResponse`
- A receiver grants a pre-vote response without setting `voted_for`; pre-vote grants do not cause term bumps in either direction

### 2.6 Election Timeout Randomization

Election timeouts are uniformly randomized in `[election_tick, 2 * election_tick)` ticks per follower reset. Randomization is seeded from `std.crypto.random` on node initialization. For deterministic testing, `Config` exposes an optional `rng_seed: ?u64 = null` field; when non-null, a seeded PRNG is used instead of the cryptographic source.

### 2.7 No-Op Entry on Leader Election

When a node transitions to leader, it immediately appends a **no-op entry** (`EntryKind.noop`, empty `data`) in its own term. This is required (Raft §8) to establish when prior-term entries are safe to commit — without it, a leader can never determine the commit point for entries from earlier terms, violating Leader Completeness.

`noop` entries are delivered in `Ready.committed_entries` with `EntryKind.noop`. Consumers MUST skip no-op entries and not apply them to the state machine. The library provides a helper:

```zig
pub fn isNoop(entry: Entry) bool {
    return entry.kind == .noop;
}
```

### 2.8 Memory Ownership

All `[]const u8` data in the public API follows **library-owns, valid-until-advance** semantics:

- **`propose(data: []const u8)`** — the library copies `data` into its internal log buffer using the allocator passed to `RaftNode.init`. The caller may free `data` immediately after `propose()` returns.
- **`Entry.data` in `Ready.committed_entries`** — slices point into library-internal log memory. Valid until `advance()` is called. Consumers that need entries beyond `advance()` must copy them.
- **`Ready.messages`** — valid until `advance()` is called. The transport implementation must copy message bytes if it buffers sends asynchronously.
- **`Snapshot.data` in `Ready.snapshot`** — valid until `advance()` is called.
- **`snapshot_fn` return value** — the library copies the returned bytes immediately. The consumer retains ownership of the original buffer.
- **`Storage.entries()` return value** — the storage implementation allocates the returned slice with the allocator passed to its `init`. The **caller** is responsible for freeing the slice with the same allocator.

All internal allocations use the `std.mem.Allocator` passed to `RaftNode.init`.

---

## 3. Architecture & Data Flow

```
Consumer
  │
  ├── propose(data: []const u8) error{NotLeader}!void  # submit command (leader only)
  │     returns error.NotLeader if not leader; read node.leader_id for redirect hint
  ├── step(msg: Message, task: ?*spice.Task)    # deliver inbound RPC
  ├── tick(task: ?*spice.Task)                  # advance logical clock
  │
  └── ready() Ready                             # drain after each tick/step/propose
        │
        ├── hard_state: ?HardState              # 1. PERSIST FIRST (write-ahead)
        ├── messages: []const Message           # 2. SEND to peers
        ├── snapshot: ?Snapshot                 # 3. APPLY snapshot (if present)
        ├── committed_entries: []const Entry    # 4. APPLY to state machine (skip .noop)
        └── should_snapshot: bool              # 5. if true, call node.reportSnapshot()
              │
              └── advance()                    # 6. release Ready memory, reset pending state
```

**Consumer contract (strict ordering invariant):**

```
loop:
  [tick | step | propose]+
  → ready()
  → persist hard_state (if non-null) to stable storage
  → send messages to peers
  → if snapshot != null: apply snapshot to state machine via restore_fn
  → for entry in committed_entries: if !isNoop(entry) then apply to state machine
  → if should_snapshot: call node.reportSnapshot(your_snapshot_fn())
  → advance()
```

Calling `ready()` before `advance()` on a previous `Ready` is a bug (panics in debug mode). Calling `propose()` or `step()` between `ready()` and `advance()` is forbidden.

**Tick / Wall-Clock Relationship:**

One `tick()` call is one logical time unit. The consumer calls `tick()` at a consistent wall-clock interval — typically every 100ms. With defaults (`election_tick=10`, `heartbeat_tick=1`):
- Heartbeat: every 100ms
- Election timeout: uniformly randomized between 1000ms and 2000ms

The library does not use wall-clock time internally. All timing is purely tick-based.

**Leader ID hint on `propose()` error:**

```zig
// propose() returns error{NotLeader}!void
// On NotLeader, consumers read node.leader_id to get the current leader hint.
pub fn propose(self: *RaftNode, data: []const u8) error{NotLeader}!void;
```

Consumers use `node.leader_id` to redirect client requests (the field is `?u64`, null if unknown).

---

## 4. Module Breakdown

```
src/
  root.zig         # public API re-exports: RaftNode, Config, Ready, Entry, Message,
                   #   Storage, Transport, Snapshot, HardState, Role, ClusterConfig
  node.zig         # RaftNode — orchestrates all modules, produces Ready
                   #   accepts *spice.Task for parallel peer message construction
                   #   appends noop entry on leader transition (§2.7)
  log.zig          # RaftLog — append, truncate at conflict (see truncation rule §4.1),
                   #   compaction, index arithmetic
  state.zig        # Role enum (follower/candidate/leader), HardState (term + voted_for only),
                   #   SoftState (role + leader_id, volatile)
  election.zig     # pre-vote + real vote: randomized timeout (§2.6), vote grant/deny,
                   #   quorum counting, pre_vote flag on wire messages
  replication.zig  # AppendEntries: build per-peer payloads (parallel w/ spice),
                   #   track next_index/match_index, commit index majority advancement
  snapshot.zig     # should_snapshot flag in Ready, reportSnapshot() entry point,
                   #   InstallSnapshot chunked send/receive, CRC32 verification,
                   #   log compaction trigger at snapshot_threshold
  membership.zig   # joint consensus: ClusterConfig, ConfigEntry, automatic phase 2
                   #   transition after joint config commits (§6.1), voter set arithmetic
  rpc.zig          # Message tagged union — all wire types (see §5)
  ready.zig        # Ready struct, advance() handshake, pending state between rounds
  storage.zig      # Storage vtable + InMemoryStorage + FileStorage (§2.2.1)
  transport.zig    # Transport vtable + InProcessTransport
  quorum.zig       # quorum math: simple majority + joint quorum
                   #   (old config ∩ new config both independently need majority)
```

All files target ≤400 lines. No file exceeds 800 lines.

#### 4.1 Log Truncation Rule

On receiving `AppendEntries`, scan entries starting at `prev_log_index + 1`:
- Keep entries whose `(index, term)` pair matches the incoming entries exactly (no rewrite needed)
- On the first mismatch (same index, different term), truncate from that index forward, then append the incoming entries
- This is distinct from "truncate at first missing entry" — matching entries are preserved

#### 4.2 Joint Consensus Phase Transitions (Automatic)

After a joint config entry (`EntryKind.config` carrying both old and new voter sets) commits, the library **automatically** appends the new-only config entry in the next `tick()`. No consumer action is required. Consumers must not call `proposeConfigChange` again during an in-progress transition (returns `error.ConfigChangePending`).

---

## 5. Core Data Types

```zig
// state.zig
pub const Role = enum { follower, candidate, leader };

// HardState: ONLY term and voted_for are persisted (commit_index is derived at runtime)
pub const HardState = struct {
    current_term: u64,
    voted_for: ?u64,  // null = not voted this term; node IDs must be >= 1 (0 is reserved/invalid)
};

// rpc.zig
pub const EntryKind = enum { noop, normal, config };

pub const Entry = struct {
    index: u64,
    term: u64,
    kind: EntryKind,
    data: []const u8,   // opaque — consumer owns serialization; empty for noop entries
};

// membership.zig
pub const ClusterConfig = struct {
    voters: []const u64,           // current (or new) voter set; node IDs >= 1
    voters_outgoing: []const u64,  // non-empty during joint consensus only
    is_joint: bool,
};

pub const SnapshotMetadata = struct {
    index: u64,
    term: u64,
    config: ClusterConfig,   // membership at snapshot point
};

pub const Snapshot = struct {
    metadata: SnapshotMetadata,
    data: []const u8,
};

// ready.zig
pub const Ready = struct {
    hard_state: ?HardState,            // persist before sending messages; null if unchanged
    snapshot: ?Snapshot,              // apply before committed_entries (from InstallSnapshot)
    committed_entries: []const Entry, // apply in order; skip entries where isNoop(e)
    messages: []const Message,        // send to peers
    // should_snapshot signals the consumer to produce a local snapshot via reportSnapshot().
    // Invariant: snapshot != null and should_snapshot == true are mutually exclusive in any
    // single Ready — a node receiving an InstallSnapshot will not simultaneously trigger
    // a local snapshot. Process snapshot (step 3) and should_snapshot (step 5) independently.
    should_snapshot: bool,
};

// node.zig
pub const Config = struct {
    id: u64,                          // must be >= 1; 0 is reserved/invalid
    peers: []const u64,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    max_entries_per_msg: u32 = 64,   // max entry count per AppendEntries message
    max_bytes_per_msg: u64 = 1024 * 1024,  // max byte size per AppendEntries (tighter wins)
    snapshot_chunk_size: usize = 64 * 1024,
    snapshot_threshold: u32 = 1000,  // entries above snapshot_index before triggering snapshot
    pre_vote: bool = true,
    rng_seed: ?u64 = null,           // null = std.crypto.random; non-null = deterministic PRNG
};

// storage.zig
pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        saveHardState: *const fn (*anyopaque, HardState) anyerror!void,
        // SAFETY-CRITICAL: loadHardState must return the last persisted term/voted_for
        // to prevent double-voting after restart (Raft safety property).
        loadHardState: *const fn (*anyopaque) anyerror!?HardState,
        appendEntries: *const fn (*anyopaque, []const Entry) anyerror!void,
        // Returns entries with indices in [lo, hi) — half-open range.
        // Returns error.Compacted if lo <= snapshot_index.
        // Returns empty slice if lo == hi.
        // Caller owns returned slice and must free it with the storage's allocator.
        // Applies tighter of max_entries_per_msg / max_bytes_per_msg limits automatically.
        entries:       *const fn (*anyopaque, lo: u64, hi: u64) anyerror![]Entry,
        term:          *const fn (*anyopaque, index: u64) anyerror!u64,
        lastIndex:     *const fn (*anyopaque) anyerror!u64,
        saveSnapshot:  *const fn (*anyopaque, Snapshot) anyerror!void,
        loadSnapshot:  *const fn (*anyopaque) anyerror!?Snapshot,
        compact:       *const fn (*anyopaque, index: u64) anyerror!void,
    };
};

// transport.zig — send() is void: silently drop on failure, Raft retries via heartbeat
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (*anyopaque, to: u64, msg: Message) void,
    };
};

// rpc.zig — wire messages
pub const RequestVote = struct {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
    pre_vote: bool,          // true = pre-vote round; receiver does not set voted_for
};

pub const RequestVoteResponse = struct {
    term: u64,
    vote_granted: bool,
    pre_vote: bool,
};

pub const AppendEntries = struct {
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const Entry,
    leader_commit: u64,
};

pub const AppendEntriesResponse = struct {
    term: u64,
    success: bool,
    // On success: index of highest replicated entry (used to advance match_index).
    // On failure: conflict_index and conflict_term provide fast log backtracking hints
    // (Raft §5.3 student guide). The leader skips directly to conflict_index rather
    // than decrementing next_index by 1 per round. conflict_term = 0 means the
    // follower has no entry at conflict_index (log too short).
    match_index: u64,       // valid when success = true
    conflict_index: u64,    // valid when success = false; first index of conflicting term
    conflict_term: u64,     // valid when success = false; term of conflicting entry (0 = none)
};

pub const InstallSnapshot = struct {
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    offset: u64,
    data: []const u8,
    done: bool,
    checksum: [32]u8,  // BLAKE3 hash of complete snapshot data (present in final chunk only)
    config: ClusterConfig,  // Cluster membership state for snapshot-based recovery
};

pub const InstallSnapshotResponse = struct {
    term: u64,
    success: bool,
};

pub const Message = union(enum) {
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
    install_snapshot: InstallSnapshot,
    install_snapshot_response: InstallSnapshotResponse,
};
```

---

## 6. Membership Changes

Joint consensus (Raft §6): when the cluster config changes, a `config` entry (`EntryKind.config`) is appended carrying both old and new voter sets in a `ClusterConfig` with `is_joint = true`. Until that entry commits, quorum requires majority from **both** old and new configs independently (`quorum.zig` handles joint quorum). After it commits, the library automatically appends a second `config` entry with `is_joint = false` carrying only the new config. At no point can two independent leaders exist.

Membership changes are exposed via:
```zig
pub fn proposeConfigChange(self: *RaftNode, new_peers: []const u64) !void;
// returns error.NotLeader, error.ConfigChangePending
```

**Leader removal:** if the new config does not include the leader's own ID, the leader steps down after the new-only config entry commits. It transitions to follower and does not disrupt the cluster during the transition window.

---

## 7. Snapshots

**Snapshot triggering (Ready pattern):** When the log grows `snapshot_threshold` entries above the last snapshot index, the library sets `Ready.should_snapshot = true`. The consumer responds by calling `node.reportSnapshot(data: []const u8)` before the next `tick()`. The library then:
1. Wraps the data in a `Snapshot` with current metadata
2. Persists via `Storage.saveSnapshot`
3. Trims the log via `Storage.compact`
4. Updates internal snapshot index/term

This keeps the library callback-free and fully consistent with the Ready pattern. The consumer's state machine serialization never blocks Raft processing.

**Lagging follower catch-up:** When a follower's `next_index` falls behind the leader's `snapshot_index`, the leader sends the snapshot in chunks via `InstallSnapshot` RPC (`snapshot_chunk_size` = 64KB default). The follower assembles chunks in order, verifies CRC32 of the complete data on the final chunk, then surfaces the `Snapshot` in `Ready.snapshot` for the consumer to apply.

---

## 8. Testing Strategy

### Tier 1 — Unit Tests (embedded per module)

Each module tested in isolation against the paper:
- `log.zig`: append, truncate at conflict (using the §4.1 rule exactly), compaction, index arithmetic
- `election.zig`: pre-vote, vote grant/deny (term check, log up-to-date check), split vote, randomized timeout range
- `replication.zig`: `next_index` backtracking, `match_index` advancement, commit majority
- `quorum.zig`: simple majority, joint quorum correctness
- `membership.zig`: joint config transitions, voter set arithmetic, automatic phase-2 trigger

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
  noop_entry_enables_prior_term_commit   # §2.7 / Raft §8

replication/
  single_entry_commits
  pipeline_entries_commit_in_order
  follower_log_conflict_truncated
  log_backtracking_on_rejection
  no_commit_from_prior_term              # Raft §5.4.2

snapshots/
  snapshot_catches_up_laggard
  chunked_snapshot_transfer
  log_compaction_after_snapshot
  should_snapshot_flag_triggers_at_threshold

membership/
  add_server_joint_consensus
  remove_server_joint_consensus
  leader_removed_steps_down
  no_double_commit_during_reconfig
  joint_phase2_automatic                 # no consumer action required

persistence/
  hard_state_survives_restart
  log_survives_restart
  snapshot_survives_restart
  torn_write_recovery                    # partial record discarded cleanly
```

### Tier 3 — Invariant Checker (debug/test builds only)

Runs after every tick in all Tier 2 and Tier 4 tests. Not compiled in ReleaseSafe/ReleaseFast. Any violation panics with full cluster state dump.

```zig
// Checker is debug/test only. O(N * log_size) per call — not for production builds.
pub const Checker = struct {
    // Election safety: at most one leader per term across all nodes
    fn checkElectionSafety(cluster: []RaftNode) void;
    // Log matching: same (index, term) → all prior entries identical
    fn checkLogMatching(cluster: []RaftNode) void;
    // Leader completeness: every committed entry appears in leader's log
    fn checkLeaderCompleteness(cluster: []RaftNode) void;
    // State machine safety: same index → same entry applied on all nodes
    fn checkStateMachineSafety(cluster: []RaftNode) void;
};
```

### Tier 4 — Chaos Tests (`tests/chaos/`)

`ChaosNetwork` wraps `InProcessTransport` with configurable fault injection:

```zig
pub const FaultConfig = struct {
    drop_rate: f64 = 0.0,
    delay_ticks: u32 = 0,
    reorder_rate: f64 = 0.0,
    partition: []const Partition = &.{},
    node_weights: []const NodeWeight = &.{},  // per-node drop_multiplier (synapse pattern)
};
```

`ChaosRunner` drives runs with a seeded RNG — the seed is printed on failure for full replay:

```zig
pub const ChaosRunner = struct {
    pub fn runTicks(self: *ChaosRunner, ticks: u64) void;
    // "Stable" = no role changes and no messages generated for stable_ticks consecutive ticks
    pub fn runUntilStable(self: *ChaosRunner, stable_ticks: u64) void;
    pub fn injectPartition(self: *ChaosRunner, partition: Partition) void;
    pub fn healPartition(self: *ChaosRunner) void;
    pub fn crashNode(self: *ChaosRunner, id: u64) void;   // clears volatile state, keeps storage
    pub fn restartNode(self: *ChaosRunner, id: u64) void;
};
```

Chaos scenarios:
```
chaos/
  flaky_network              — 10%/30%/50% drop rates, 10k ticks, no committed entry lost
  message_reorder            — high reorder rate, log matching invariant never violated
  sustained_partition        — 3|2 split: minority can't commit, majority does; heal + catchup
  leader_isolation           — partition leader; new leader elected; old steps down on rejoin
  rolling_restart            — cycle each node crash+restart, no committed entry lost
  cascade_failures           — kill 2 of 5 simultaneously, cluster maintains quorum
  flapping_connection        — repeatedly partition/heal same link, verify stability
  slow_follower              — 10% message delivery, follower catches up via snapshot
  election_storm             — force simultaneous elections, pre-vote breaks storm
  membership_under_chaos     — add/remove server under message loss, joint consensus correct
```

Invariant `Checker` (Tier 3) runs after every single tick throughout all chaos scenarios.

---

## 9. Build & Dependencies

`build.zig.zon` adds spice as a path dependency (update required as part of implementation setup):
```zig
.dependencies = .{
    .spice = .{ .path = "../spice" },
},
```

Note: path dependencies require consumers to have `spice` checked out at the same relative path. A future release should add a `url` + `hash` entry for reproducible builds.

`build.zig` additions:
- `test-chaos` step accepting `-Dseed=N` for deterministic replay
- `check` step (existing, no change)

Build targets:
- `zig build test` — unit tests (Tier 1)
- `zig build test-integration` — scenario tests (Tier 2)
- `zig build test-chaos` — chaos tests (Tier 4); `-Dseed=N` for replay
- `zig build check` — syntax only (IDE)

---

## 10. File Count Summary

| Category | Files | Estimated Lines |
|----------|-------|----------------|
| Source modules | 13 | ~3,500 |
| Unit tests (embedded) | — | ~1,000 |
| Scenario tests | ~22 files | ~2,200 |
| Chaos tests | ~10 files | ~1,000 |
| **Total** | | **~7,700** |
