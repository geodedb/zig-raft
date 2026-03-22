# Raft Library — Implementation Plan

**Date:** 2026-03-21
**Spec:** `docs/superpowers/specs/2026-03-21-raft-design.md`
**Status:** Draft

---

## Overview

13 source modules, ~7,700 estimated lines. Implementation proceeds bottom-up: foundational data types and utilities first, then core algorithm modules, then the orchestrating node, and finally tests.

Spice is available at `../spice` as a path dependency.

---

## Phase 1: Foundation (No Dependencies Between Files)

These modules have zero internal dependencies beyond `std`. They can be implemented in parallel.

### 1.1 `src/state.zig` — Role & HardState (~60 lines)

**Creates:** `Role`, `HardState`, `SoftState`

```zig
pub const Role = enum { follower, candidate, leader };

pub const HardState = struct {
    current_term: u64,
    voted_for: ?u64,

    pub fn eql(a: HardState, b: HardState) bool;
};

pub const SoftState = struct {
    role: Role,
    leader_id: ?u64,
};
```

- `HardState.eql()` — used by `ready.zig` to detect changes
- No methods beyond equality check
- **Tests:** equality, default values

### 1.2 `src/rpc.zig` — Message Types & Entry (~120 lines)

**Creates:** `EntryKind`, `Entry`, all RPC structs, `Message` tagged union

Per spec §5 exactly. Key additions beyond the spec struct definitions:

```zig
pub fn isNoop(entry: Entry) bool {
    return entry.kind == .noop;
}
```

- `Message` is `union(enum)` with all 6 RPC types
- `Entry` has `index`, `term`, `kind`, `data`
- **Tests:** `isNoop` helper, tagged union field access

### 1.3 `src/quorum.zig` — Quorum Math (~80 lines)

**Creates:** Simple majority + joint quorum functions

```zig
/// Returns true if `votes` constitute a majority of `voters`.
pub fn hasMajority(voters: []const u64, votes: []const u64) bool;

/// Joint quorum: both old AND new voter sets must independently have majority.
pub fn hasJointMajority(
    voters: []const u64,
    voters_outgoing: []const u64,
    votes: []const u64,
) bool;

/// Compute commit index from match_index values using majority agreement.
/// For joint consensus, requires majority in both voter sets.
pub fn committedIndex(
    voters: []const u64,
    voters_outgoing: []const u64,
    match_indices: []const u64,
    node_ids: []const u64,
    current_commit: u64,
) u64;
```

- `committedIndex` finds the highest N such that a majority of nodes have `match_index >= N`
- Joint quorum requires majority in **both** sets independently
- **Tests:** odd/even cluster sizes, joint quorum edge cases, committed index advancement

---

## Phase 2: Storage & Transport (Depends on Phase 1 types)

### 2.1 `src/storage.zig` — Storage Vtable + InMemoryStorage (~250 lines)

**Creates:** `Storage` vtable, `InMemoryStorage`

**Vtable** (per spec §5):
- `saveHardState`, `appendEntries`, `entries(lo, hi)`, `term(index)`, `lastIndex`
- `saveSnapshot`, `loadSnapshot`, `compact(index)`

**`InMemoryStorage`:**
- Backed by `std.ArrayList(Entry)` for log, `?Snapshot` for snapshot, `?HardState`
- `entries()` returns a caller-owned slice (allocated with the storage's allocator)
- `compact()` trims entries up to and including the given index
- Tracks `snapshot_index` / `snapshot_term` for compaction boundary
- `entries()` returns `error.Compacted` if `lo <= snapshot_index`

**FileStorage deferred to Phase 7** — not needed for algorithm correctness; InMemoryStorage is sufficient for all test tiers.

```zig
pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct { ... }; // per spec

    // Convenience wrappers that dispatch through vtable
    pub fn saveHardState(self: Storage, hs: HardState) anyerror!void;
    pub fn appendEntries(self: Storage, entries: []const Entry) anyerror!void;
    pub fn entries(self: Storage, lo: u64, hi: u64) anyerror![]Entry;
    // ... etc
};

pub const InMemoryStorage = struct {
    pub fn init(allocator: std.mem.Allocator) InMemoryStorage;
    pub fn deinit(self: *InMemoryStorage) void;
    pub fn storage(self: *InMemoryStorage) Storage; // returns vtable wrapper
};
```

- **Tests:** append/retrieve round-trip, compaction, `error.Compacted`, snapshot save/load, hard state persistence

### 2.2 `src/transport.zig` — Transport Vtable + InProcessTransport (~120 lines)

**Creates:** `Transport` vtable, `InProcessTransport`

```zig
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (*anyopaque, to: u64, msg: Message) void,
    };

    pub fn send(self: Transport, to: u64, msg: Message) void;
};

pub const InProcessTransport = struct {
    /// Maps node_id -> inbox (bounded channel or ArrayList)
    pub fn init(allocator: std.mem.Allocator) InProcessTransport;
    pub fn deinit(self: *InProcessTransport) void;
    pub fn register(self: *InProcessTransport, id: u64) void;
    pub fn transport(self: *InProcessTransport, from: u64) Transport;
    /// Drain inbox for a node — used by test harness
    pub fn drain(self: *InProcessTransport, id: u64) []Message;
};
```

- `send()` is void — silently drops if target not registered (per spec §2.3)
- Messages stored in `std.ArrayList(Message)` per node
- **Tests:** send/drain round-trip, drop on unknown target

---

## Phase 3: Log Management (Depends on Phase 1 + 2)

### 3.1 `src/log.zig` — RaftLog (~300 lines)

**Creates:** `RaftLog` — wraps `Storage` with in-memory unstable entries and index arithmetic

```zig
pub const RaftLog = struct {
    storage: Storage,
    unstable_entries: std.ArrayList(Entry),
    committed: u64,
    applied: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, storage: Storage) !RaftLog;
    pub fn deinit(self: *RaftLog) void;

    /// Append entries; copies data into internal buffer.
    pub fn append(self: *RaftLog, entries: []const Entry) !void;

    /// Last log index (stable + unstable).
    pub fn lastIndex(self: *RaftLog) u64;

    /// Last log term.
    pub fn lastTerm(self: *RaftLog) u64;

    /// Term at given index. Returns error.Compacted if below snapshot.
    pub fn term(self: *RaftLog, index: u64) !u64;

    /// Entries in [lo, hi) range across stable + unstable.
    pub fn slice(self: *RaftLog, lo: u64, hi: u64) ![]Entry;

    /// Truncate-and-append per §4.1: find first conflict, truncate from there, append rest.
    pub fn maybeAppend(
        self: *RaftLog,
        prev_log_index: u64,
        prev_log_term: u64,
        committed: u64,
        entries: []const Entry,
    ) !?u64; // returns last new entry index, or null if prev doesn't match

    /// Find conflict info for AppendEntriesResponse fast backtracking.
    pub fn findConflict(self: *RaftLog, index: u64) struct { index: u64, term: u64 };

    /// Advance commit index to min(committed, lastIndex).
    pub fn commitTo(self: *RaftLog, committed: u64) void;

    /// Mark entries as applied up to index.
    pub fn appliedTo(self: *RaftLog, index: u64) void;

    /// Persist unstable entries to storage and clear unstable buffer.
    pub fn stableTo(self: *RaftLog, index: u64, term_val: u64) !void;

    /// Compact log up to snapshot index.
    pub fn compact(self: *RaftLog, compact_index: u64) !void;

    /// Restore from snapshot (reset log state).
    pub fn restore(self: *RaftLog, snapshot: Snapshot) !void;

    /// Check if committing to `index` is allowed (term-based safety per §5.4.2).
    pub fn matchTerm(self: *RaftLog, index: u64, term_val: u64) bool;

    /// Returns true if this log is at least as up-to-date as (last_log_index, last_log_term).
    pub fn isUpToDate(self: *RaftLog, last_log_index: u64, last_log_term: u64) bool;

    /// Returns entries that need to be persisted (unstable entries not yet in storage).
    pub fn unstableEntries(self: *RaftLog) []const Entry;

    /// Returns whether there's a snapshot to apply.
    pub fn hasPendingSnapshot(self: *RaftLog) bool;
};
```

Key behaviors:
- **Truncation rule (§4.1):** `maybeAppend` scans from `prev_log_index + 1`, keeps matching `(index, term)` pairs, truncates on first mismatch
- **Conflict detection:** `findConflict` returns `conflict_index` / `conflict_term` for fast backtracking
- **Commit safety (§5.4.2):** only advance commit if the entry at the new commit index has the current leader's term
- **Tests:** append, truncation at conflict, commit advancement, compaction, snapshot restore, `isUpToDate` comparisons, `findConflict` edge cases

---

## Phase 4: Algorithm Modules (Depends on Phase 1-3)

### 4.1 `src/election.zig` — Election Logic (~250 lines)

**Creates:** Election state machine — pre-vote + real vote handling

```zig
pub const ElectionState = struct {
    election_tick: u32,
    heartbeat_tick: u32,
    election_timeout: u32,     // randomized per reset
    heartbeat_elapsed: u32,
    election_elapsed: u32,
    votes_received: std.AutoHashMap(u64, bool), // node_id -> granted
    random: std.Random,

    pub fn init(
        allocator: std.mem.Allocator,
        election_tick: u32,
        heartbeat_tick: u32,
        rng_seed: ?u64,
    ) ElectionState;
    pub fn deinit(self: *ElectionState) void;

    /// Randomize election timeout in [election_tick, 2 * election_tick).
    pub fn resetElectionTimeout(self: *ElectionState) void;

    /// Returns true if election timeout has elapsed.
    pub fn electionTimeoutElapsed(self: *ElectionState) bool;

    /// Returns true if heartbeat interval has elapsed.
    pub fn heartbeatTimeoutElapsed(self: *ElectionState) bool;

    /// Tick the election timer (increment elapsed counters).
    pub fn tickElection(self: *ElectionState) void;
    pub fn tickHeartbeat(self: *ElectionState) void;

    /// Reset election elapsed (called on receiving valid AppendEntries or granting vote).
    pub fn resetElectionElapsed(self: *ElectionState) void;

    /// Record a vote and return whether quorum is reached.
    pub fn recordVote(self: *ElectionState, from: u64, granted: bool) void;
    pub fn hasQuorum(self: *ElectionState, voters: []const u64) bool;
    pub fn hasJointQuorum(
        self: *ElectionState,
        voters: []const u64,
        voters_outgoing: []const u64,
    ) bool;

    /// Reset votes for a new election round.
    pub fn resetVotes(self: *ElectionState) void;

    /// Check if this node should grant a vote to the candidate.
    /// Per Raft: grant if (voted_for is null or == candidate_id) AND candidate log is up-to-date.
    pub fn shouldGrantVote(
        voted_for: ?u64,
        candidate_id: u64,
        candidate_last_index: u64,
        candidate_last_term: u64,
        local_last_index: u64,
        local_last_term: u64,
        pre_vote: bool,
    ) bool;
};
```

- Randomization uses `std.crypto.random` by default, seeded PRNG when `rng_seed != null`
- `shouldGrantVote` is a pure function — no side effects, used for both pre-vote and real vote
- Pre-vote: caller does NOT set `voted_for` or bump term
- **Tests:** timeout randomization range, vote grant/deny logic, quorum counting, pre-vote vs real vote behavior

### 4.2 `src/replication.zig` — Replication State (~250 lines)

**Creates:** Per-peer replication tracking, AppendEntries construction, commit advancement

```zig
pub const PeerProgress = struct {
    next_index: u64,
    match_index: u64,

    /// After rejection, update next_index using conflict hints.
    pub fn maybeDecrTo(
        self: *PeerProgress,
        rejected_index: u64,
        conflict_index: u64,
        conflict_term: u64,
        log: *RaftLog,
    ) bool; // returns true if next_index changed

    /// After success, update match_index and next_index.
    pub fn maybeUpdate(self: *PeerProgress, match: u64) bool;
};

pub const ReplicationState = struct {
    peers: std.AutoHashMap(u64, PeerProgress),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, peer_ids: []const u64, last_index: u64) !ReplicationState;
    pub fn deinit(self: *ReplicationState) void;

    /// Reset all peers' next_index to last_index + 1 (on becoming leader).
    pub fn resetPeers(self: *ReplicationState, peer_ids: []const u64, last_index: u64) !void;

    /// Build AppendEntries message for a specific peer.
    pub fn buildAppendEntries(
        self: *ReplicationState,
        peer_id: u64,
        log: *RaftLog,
        term: u64,
        leader_id: u64,
        commit_index: u64,
        max_entries: u32,
        max_bytes: u64,
    ) !?Message; // null if peer needs snapshot instead

    /// Build AppendEntries messages for ALL peers (parallelizable with spice).
    pub fn buildAllAppendEntries(
        self: *ReplicationState,
        log: *RaftLog,
        term: u64,
        leader_id: u64,
        commit_index: u64,
        max_entries: u32,
        max_bytes: u64,
        peer_ids: []const u64,
        task: ?*spice.Task,
    ) ![]Message;

    /// Compute new commit index based on majority match_index.
    pub fn maybeCommit(
        self: *ReplicationState,
        log: *RaftLog,
        voters: []const u64,
        voters_outgoing: []const u64,
        current_term: u64,
    ) bool; // returns true if commit index advanced
};
```

- `maybeDecrTo` implements fast backtracking per spec §5 (`conflict_index` / `conflict_term`)
- `maybeCommit` enforces §5.4.2: only commits entries from the current term
- `buildAllAppendEntries` uses `spice.Task` for parallel construction when provided
- **Tests:** next_index backtracking, match_index advancement, commit majority calculation, §5.4.2 safety

### 4.3 `src/membership.zig` — Joint Consensus (~200 lines)

**Creates:** `ClusterConfig`, config change logic, automatic phase-2

```zig
pub const ClusterConfig = struct {
    voters: []const u64,
    voters_outgoing: []const u64,  // non-empty during joint consensus
    is_joint: bool,

    pub fn isVoter(self: ClusterConfig, id: u64) bool;
    pub fn allVoters(self: ClusterConfig, allocator: std.mem.Allocator) ![]u64;
};

pub const MembershipState = struct {
    config: ClusterConfig,
    pending_config_change: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, initial_voters: []const u64) !MembershipState;
    pub fn deinit(self: *MembershipState) void;

    /// Start a config change: creates joint config entry.
    /// Returns error.ConfigChangePending if one is already in progress.
    pub fn proposeChange(self: *MembershipState, new_voters: []const u64) !Entry;

    /// After joint config commits, produce the phase-2 (new-only) config entry.
    pub fn finalizeChange(self: *MembershipState) ?Entry;

    /// Apply a committed config entry (update voter sets).
    pub fn applyConfig(self: *MembershipState, entry: Entry) !void;

    /// Check if leader should step down (not in new config).
    pub fn shouldLeaderStepDown(self: *MembershipState, leader_id: u64) bool;
};
```

- Joint config entry: `EntryKind.config`, data = serialized `ClusterConfig` with both voter sets
- Phase-2 automatic: after joint config commits, `finalizeChange()` returns new-only entry; node appends it in next `tick()`
- **Tests:** joint config creation, phase-2 generation, voter set arithmetic, leader step-down detection

### 4.4 `src/snapshot.zig` — Snapshot Management (~200 lines)

**Creates:** Snapshot triggering, chunked transfer, CRC verification

```zig
pub const SnapshotState = struct {
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_threshold: u32,
    chunk_size: usize,
    /// Receiving state for chunked InstallSnapshot
    pending_snapshot: ?PendingSnapshot,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        threshold: u32,
        chunk_size: usize,
    ) SnapshotState;
    pub fn deinit(self: *SnapshotState) void;

    /// Check if log has grown enough above snapshot to trigger.
    pub fn shouldSnapshot(self: *SnapshotState, last_index: u64) bool;

    /// Process reportSnapshot() from consumer — wrap data, persist, compact.
    pub fn applyLocalSnapshot(
        self: *SnapshotState,
        data: []const u8,
        last_included_index: u64,
        last_included_term: u64,
        config: ClusterConfig,
        storage: Storage,
    ) !Snapshot;

    /// Build InstallSnapshot chunks for sending to a lagging follower.
    pub fn buildInstallSnapshotMessages(
        self: *SnapshotState,
        snapshot: Snapshot,
        term: u64,
        leader_id: u64,
    ) ![]Message;

    /// Receive an InstallSnapshot chunk. Returns completed Snapshot on final chunk.
    pub fn receiveChunk(self: *SnapshotState, msg: InstallSnapshot) !?Snapshot;
};

const PendingSnapshot = struct {
    data: std.ArrayList(u8),
    last_included_index: u64,
    last_included_term: u64,
    expected_offset: u64,
};
```

- `shouldSnapshot`: returns `true` when `last_index - snapshot_index >= snapshot_threshold`
- CRC32 verification on final chunk of InstallSnapshot
- **Tests:** threshold triggering, chunk assembly, CRC verification, partial/out-of-order chunk rejection

---

## Phase 5: Ready & Node (Depends on Phase 1-4)

### 5.1 `src/ready.zig` — Ready Struct & Advance Handshake (~100 lines)

**Creates:** `Ready` struct, pending state tracking

```zig
pub const Ready = struct {
    hard_state: ?HardState,
    snapshot: ?Snapshot,
    committed_entries: []const Entry,
    messages: []const Message,
    should_snapshot: bool,
};

pub const ReadyState = struct {
    prev_hard_state: HardState,
    has_pending_ready: bool,
    pending_messages: std.ArrayList(Message),
    pending_committed_entries: std.ArrayList(Entry),
    pending_snapshot: ?Snapshot,
    pending_should_snapshot: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, initial_hs: HardState) ReadyState;
    pub fn deinit(self: *ReadyState) void;

    /// Accumulate a message to be included in the next Ready.
    pub fn addMessage(self: *ReadyState, msg: Message) !void;
    pub fn addMessages(self: *ReadyState, msgs: []const Message) !void;

    /// Accumulate committed entries.
    pub fn addCommittedEntries(self: *ReadyState, entries: []const Entry) !void;

    /// Set snapshot to deliver.
    pub fn setSnapshot(self: *ReadyState, snapshot: Snapshot) void;

    /// Build Ready from accumulated state. Panics if called before advance() on previous Ready.
    pub fn ready(self: *ReadyState, current_hs: HardState) Ready;

    /// Release Ready memory, reset pending state.
    pub fn advance(self: *ReadyState) void;

    /// Returns true if there's anything to deliver.
    pub fn hasReady(self: *ReadyState, current_hs: HardState) bool;
};
```

- `ready()` panics (debug) if `has_pending_ready == true` (called before `advance()`)
- `hard_state` is only non-null when it differs from `prev_hard_state`
- **Tests:** ready/advance handshake, panic on double-ready, hard_state change detection

### 5.2 `src/node.zig` — RaftNode Orchestrator (~400 lines)

**Creates:** `RaftNode` — the main public API, orchestrates all modules

```zig
pub const RaftNode = struct {
    id: u64,
    role: Role,
    leader_id: ?u64,
    log: RaftLog,
    election: ElectionState,
    replication: ?ReplicationState,  // non-null only when leader
    membership: MembershipState,
    snapshots: SnapshotState,
    ready_state: ReadyState,
    config: Config,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: Config, storage: Storage) !RaftNode;
    pub fn deinit(self: *RaftNode) void;

    // --- Consumer API ---

    /// Submit a command for replication (leader only).
    pub fn propose(self: *RaftNode, data: []const u8) !void;
    // Returns error.NotLeader with leader_id hint

    /// Propose a membership change (leader only).
    pub fn proposeConfigChange(self: *RaftNode, new_peers: []const u64) !void;
    // Returns error.NotLeader, error.ConfigChangePending

    /// Deliver an inbound RPC message.
    pub fn step(self: *RaftNode, msg: Message, task: ?*spice.Task) !void;

    /// Advance the logical clock by one tick.
    pub fn tick(self: *RaftNode, task: ?*spice.Task) !void;

    /// Drain accumulated state into a Ready struct.
    pub fn ready(self: *RaftNode) Ready;

    /// Release Ready memory, allowing next round.
    pub fn advance(self: *RaftNode) void;

    /// Report a snapshot produced by the consumer.
    pub fn reportSnapshot(self: *RaftNode, data: []const u8) !void;

    // --- Internal state transitions ---

    fn becomeFollower(self: *RaftNode, term: u64, leader_id: ?u64) void;
    fn becomeCandidate(self: *RaftNode) !void;
    fn becomeLeader(self: *RaftNode) !void;

    // --- Message handlers ---

    fn handleRequestVote(self: *RaftNode, msg: RequestVote) !void;
    fn handleRequestVoteResponse(self: *RaftNode, msg: RequestVoteResponse) !void;
    fn handleAppendEntries(self: *RaftNode, msg: AppendEntries) !void;
    fn handleAppendEntriesResponse(self: *RaftNode, msg: AppendEntriesResponse) !void;
    fn handleInstallSnapshot(self: *RaftNode, msg: InstallSnapshot) !void;
    fn handleInstallSnapshotResponse(self: *RaftNode, msg: InstallSnapshotResponse) !void;
};
```

Key orchestration logic in `tick()`:
1. If follower/candidate: tick election timer; if elapsed, start (pre-)election
2. If leader: tick heartbeat timer; if elapsed, send heartbeats to all peers
3. If leader + joint config just committed: auto-append phase-2 config entry
4. If leader + new-only config committed + leader not in new config: step down

Key orchestration logic in `becomeLeader()`:
1. Append no-op entry (§2.7)
2. Initialize `ReplicationState` with all peers
3. Reset heartbeat timer

`step()` dispatches to the appropriate handler based on `Message` tag.

- **Tests:** state transitions, no-op on leader election, propose error on non-leader, full tick/step/ready/advance cycle

### 5.3 `src/root.zig` — Public API Re-exports (~40 lines)

**Modifies:** existing `src/root.zig`

Replace the template content with re-exports:

```zig
pub const state = @import("state.zig");
pub const rpc = @import("rpc.zig");
pub const quorum = @import("quorum.zig");
pub const storage = @import("storage.zig");
pub const transport = @import("transport.zig");
pub const log = @import("log.zig");
pub const election = @import("election.zig");
pub const replication = @import("replication.zig");
pub const membership = @import("membership.zig");
pub const snapshot = @import("snapshot.zig");
pub const ready = @import("ready.zig");
pub const node = @import("node.zig");

// Top-level convenience re-exports
pub const RaftNode = node.RaftNode;
pub const Config = node.Config;
pub const Ready = ready.Ready;
pub const Entry = rpc.Entry;
pub const EntryKind = rpc.EntryKind;
pub const Message = rpc.Message;
pub const Role = state.Role;
pub const HardState = state.HardState;
pub const Storage = storage.Storage;
pub const InMemoryStorage = storage.InMemoryStorage;
pub const Transport = transport.Transport;
pub const InProcessTransport = transport.InProcessTransport;
pub const Snapshot = rpc.Snapshot; // or snapshot module
pub const ClusterConfig = membership.ClusterConfig;
pub const isNoop = rpc.isNoop;
```

---

## Phase 6: Build System Changes

### 6.1 `build.zig.zon` — Add Spice Dependency

```zig
.dependencies = .{
    .spice = .{ .path = "../spice" },
},
```

### 6.2 `build.zig` — Wire Spice + Add Test Steps

Changes:
1. Import spice dependency and add it to the main raft module
2. Add `test-chaos` step with `-Dseed=N` option
3. Wire spice into all test compilations

```zig
// Add spice dependency to main module
const spice_dep = b.dependency("spice", .{
    .target = target,
    .optimize = optimize,
});
module.addImport("spice", spice_dep.module("spice"));

// Chaos test step with seed option
const chaos_step = b.step("test-chaos", "Run chaos tests");
const seed_option = b.option(u64, "seed", "Deterministic seed for chaos tests");
const chaos_tests = b.addTest(.{
    .root_module = b.createModule(.{
        .root_source_file = b.path("tests/chaos.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "raft", .module = module },
        },
    }),
});
if (seed_option) |seed| {
    chaos_tests.root_module.addOptions("chaos_options", ...); // pass seed
}
```

---

## Phase 7: FileStorage (Depends on Phase 2)

### 7.1 `src/file_storage.zig` — Crash-Safe File Storage (~350 lines)

**Creates:** `FileStorage` implementing `Storage` vtable

Per spec §2.2.1:
- Record format: `[4-byte LE length][Entry bytes][4-byte CRC32]`
- Torn write detection on open: scan forward, truncate partial
- `.idx` file for O(1) random access
- `appendEntries`: write to temp file, `fdatasync`, `rename`
- `compact(index)`: rewrite from `index + 1`, `fdatasync`, `rename`

This is deferred because InMemoryStorage is sufficient for all algorithm testing. FileStorage is a production concern.

- **Tests:** write/read round-trip, torn write recovery, index rebuild, compact atomicity

---

## Phase 8: Test Infrastructure (Depends on Phase 5)

### 8.1 `tests/harness.zig` — Test Harness (~200 lines)

**Creates:** `Network` simulator wrapping `InProcessTransport`

```zig
pub const Network = struct {
    nodes: std.AutoHashMap(u64, *RaftNode),
    transport: InProcessTransport,
    storages: std.AutoHashMap(u64, *InMemoryStorage),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, node_count: u32) !Network;
    pub fn deinit(self: *Network) void;

    /// Tick all nodes once and deliver all pending messages.
    pub fn tickAll(self: *Network) !void;

    /// Run ticks until a leader is elected or max_ticks exceeded.
    pub fn waitForLeader(self: *Network, max_ticks: u32) !u64;

    /// Propose a command on the leader.
    pub fn propose(self: *Network, data: []const u8) !void;

    /// Run ticks until the command is committed on a majority.
    pub fn waitForCommit(self: *Network, index: u64, max_ticks: u32) !void;

    /// Drop messages between two nodes (bidirectional partition).
    pub fn partition(self: *Network, a: u64, b: u64) void;
    pub fn heal(self: *Network, a: u64, b: u64) void;

    /// Pause a node (skip its tick/step processing).
    pub fn pauseNode(self: *Network, id: u64) void;
    pub fn resumeNode(self: *Network, id: u64) void;

    /// Get committed entries on a specific node.
    pub fn committedEntries(self: *Network, id: u64) []const Entry;
};
```

### 8.2 `tests/checker.zig` — Invariant Checker (~150 lines)

**Creates:** Raft invariant checker (debug/test builds only)

Per spec §8 Tier 3:
- `checkElectionSafety` — at most one leader per term
- `checkLogMatching` — same (index, term) implies all prior entries identical
- `checkLeaderCompleteness` — every committed entry in leader's log
- `checkStateMachineSafety` — same index implies same entry across nodes

Called after every `tickAll()` in the Network harness.

---

## Phase 9: Scenario Tests (Depends on Phase 8)

### 9.1 `tests/integration.zig` — Entry Point

Imports all scenario test files:

```zig
comptime {
    _ = @import("scenarios/election.zig");
    _ = @import("scenarios/replication.zig");
    _ = @import("scenarios/snapshots.zig");
    _ = @import("scenarios/membership.zig");
    _ = @import("scenarios/persistence.zig");
}
```

### 9.2 Scenario Test Files

Per spec §8 Tier 2, organized by category:

| File | Tests |
|------|-------|
| `tests/scenarios/election.zig` | basic_leader_election, split_vote_resolves, pre_vote_prevents_disruption, leader_steps_down_on_stale_term, no_double_vote_same_term, noop_entry_enables_prior_term_commit |
| `tests/scenarios/replication.zig` | single_entry_commits, pipeline_entries_commit_in_order, follower_log_conflict_truncated, log_backtracking_on_rejection, no_commit_from_prior_term |
| `tests/scenarios/snapshots.zig` | snapshot_catches_up_laggard, chunked_snapshot_transfer, log_compaction_after_snapshot, should_snapshot_flag_triggers_at_threshold |
| `tests/scenarios/membership.zig` | add_server_joint_consensus, remove_server_joint_consensus, leader_removed_steps_down, no_double_commit_during_reconfig, joint_phase2_automatic |
| `tests/scenarios/persistence.zig` | hard_state_survives_restart, log_survives_restart, snapshot_survives_restart, torn_write_recovery |

---

## Phase 10: Chaos Tests (Depends on Phase 8)

### 10.1 `tests/chaos.zig` — Chaos Entry Point

```zig
comptime {
    _ = @import("chaos/runner.zig");
    _ = @import("chaos/scenarios.zig");
}
```

### 10.2 `tests/chaos/runner.zig` — ChaosRunner (~200 lines)

Per spec §8 Tier 4:

```zig
pub const FaultConfig = struct {
    drop_rate: f64 = 0.0,
    delay_ticks: u32 = 0,
    reorder_rate: f64 = 0.0,
    partition: []const Partition = &.{},
    node_weights: []const NodeWeight = &.{},
};

pub const ChaosRunner = struct {
    network: Network,
    fault_config: FaultConfig,
    rng: std.Random,
    seed: u64,

    pub fn init(allocator: std.mem.Allocator, node_count: u32, seed: u64, config: FaultConfig) !ChaosRunner;
    pub fn runTicks(self: *ChaosRunner, ticks: u64) !void;
    pub fn runUntilStable(self: *ChaosRunner, stable_ticks: u64) !void;
    pub fn injectPartition(self: *ChaosRunner, partition: Partition) void;
    pub fn healPartition(self: *ChaosRunner) void;
    pub fn crashNode(self: *ChaosRunner, id: u64) !void;
    pub fn restartNode(self: *ChaosRunner, id: u64) !void;
};
```

### 10.3 `tests/chaos/scenarios.zig` — Chaos Scenarios (~400 lines)

Per spec §8 Tier 4: flaky_network, message_reorder, sustained_partition, leader_isolation, rolling_restart, cascade_failures, flapping_connection, slow_follower, election_storm, membership_under_chaos.

Each scenario prints seed on failure for full replay.

---

## Implementation Order & Dependencies

```
Phase 1 (parallel)
  ├── 1.1 state.zig
  ├── 1.2 rpc.zig
  └── 1.3 quorum.zig

Phase 2 (parallel, depends on Phase 1)
  ├── 2.1 storage.zig
  └── 2.2 transport.zig

Phase 3 (depends on Phase 1 + 2)
  └── 3.1 log.zig

Phase 4 (partially parallel, depends on Phase 1-3)
  ├── 4.1 election.zig     (depends on state, rpc, quorum)
  ├── 4.2 replication.zig   (depends on state, rpc, quorum, log, spice)
  ├── 4.3 membership.zig    (depends on state, rpc, quorum)
  └── 4.4 snapshot.zig      (depends on state, rpc, storage)

Phase 5 (sequential, depends on Phase 1-4)
  ├── 5.1 ready.zig
  ├── 5.2 node.zig          (depends on ALL modules)
  └── 5.3 root.zig          (re-exports)

Phase 6 (can start after Phase 1, finalize after Phase 5)
  ├── 6.1 build.zig.zon
  └── 6.2 build.zig

Phase 7 (independent, after Phase 2)
  └── 7.1 file_storage.zig

Phase 8 (depends on Phase 5)
  ├── 8.1 harness.zig
  └── 8.2 checker.zig

Phase 9 (depends on Phase 8)
  └── 9.1-9.2 scenario tests

Phase 10 (depends on Phase 8)
  └── 10.1-10.3 chaos tests
```

---

## Critical Correctness Points

These are the areas where bugs would violate Raft safety properties. Extra care and testing required:

1. **§5.4.2 — No commit from prior term:** `maybeCommit` must check that the entry at the proposed commit index has `term == current_term`. This is the most subtle safety invariant.

2. **§4.1 — Log truncation rule:** Must NOT truncate matching entries. Only truncate from the first `(index, term)` mismatch forward.

3. **§2.7 — No-op on leader election:** Without the no-op, the leader cannot determine when prior-term entries are safe to commit. This MUST happen in `becomeLeader()`.

4. **Pre-vote isolation:** Pre-vote requests must NOT modify `voted_for` or `current_term` on either the sender or receiver. The `pre_vote: bool` field on wire messages is the only differentiator.

5. **Joint quorum:** During joint consensus, quorum requires majority from BOTH old and new configs independently. A majority of the union is NOT sufficient.

6. **Memory ownership:** `Ready` slices are only valid until `advance()`. The library must copy `propose()` data immediately. `Storage.entries()` returns caller-owned memory.

7. **`advance()` before `ready()`:** Calling `ready()` twice without `advance()` is a bug. Debug-mode panic.

---

## File Inventory

| Path | Phase | Lines (est) | New/Modify |
|------|-------|-------------|------------|
| `src/state.zig` | 1.1 | 60 | New |
| `src/rpc.zig` | 1.2 | 120 | New |
| `src/quorum.zig` | 1.3 | 80 | New |
| `src/storage.zig` | 2.1 | 250 | New |
| `src/transport.zig` | 2.2 | 120 | New |
| `src/log.zig` | 3.1 | 300 | New |
| `src/election.zig` | 4.1 | 250 | New |
| `src/replication.zig` | 4.2 | 250 | New |
| `src/membership.zig` | 4.3 | 200 | New |
| `src/snapshot.zig` | 4.4 | 200 | New |
| `src/ready.zig` | 5.1 | 100 | New |
| `src/node.zig` | 5.2 | 400 | New |
| `src/root.zig` | 5.3 | 40 | Modify |
| `src/file_storage.zig` | 7.1 | 350 | New |
| `build.zig` | 6.2 | ~60 | Modify |
| `build.zig.zon` | 6.1 | ~5 | Modify |
| `tests/harness.zig` | 8.1 | 200 | New |
| `tests/checker.zig` | 8.2 | 150 | New |
| `tests/integration.zig` | 9.1 | 10 | Modify (exists) |
| `tests/scenarios/election.zig` | 9.2 | 300 | New |
| `tests/scenarios/replication.zig` | 9.2 | 250 | New |
| `tests/scenarios/snapshots.zig` | 9.2 | 250 | New |
| `tests/scenarios/membership.zig` | 9.2 | 250 | New |
| `tests/scenarios/persistence.zig` | 9.2 | 200 | New |
| `tests/chaos.zig` | 10.1 | 10 | New |
| `tests/chaos/runner.zig` | 10.2 | 200 | New |
| `tests/chaos/scenarios.zig` | 10.3 | 400 | New |
| **Total** | | **~4,995** | |

Note: Spec estimated ~7,700 lines. The delta is in embedded unit tests (~1,000 lines) which are included within each source module but not counted separately above, plus test helper code.
