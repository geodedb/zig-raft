// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const tp = @import("thread_pool");
const st = @import("state.zig");
const rpc = @import("rpc.zig");
const raft_log = @import("log.zig");
const elect = @import("election.zig");
const repl = @import("replication.zig");
const mem = @import("membership.zig");
const snap = @import("snapshot.zig");
const rd = @import("ready.zig");
const stg = @import("storage.zig");
const trans = @import("transport.zig");
// Observability seam (otel). Comptime no-op when -Dwith_otel=false; the
// compiler eliminates every span-related instruction at the call sites
// below. When the flag is on, each helper forwards to a tracer-bound
// `otel.api.trace.Span` with `rpc.system="raft"` semantic conventions.
const observability = @import("observability");

/// Maximum number of peers that can be served from a stack buffer
/// without heap allocation (PERF-004, QA-006). Covers clusters up to
/// 16 voters which is well beyond practical Raft deployments (3-7).
pub const MAX_STACK_PEERS: usize = 16;

/// Heartbeat rounds to wait before re-sending an in-flight InstallSnapshot to a
/// peer that has not yet acknowledged it. Bounds wasted bandwidth on the common
/// case (ack arrives within a round or two) while still recovering from a lost
/// snapshot or a follower that acked then crashed (SEC-0144).
pub const SNAPSHOT_RESEND_TICKS: u32 = 8;

/// The longest wait, in heartbeat rounds, before a refused InstallSnapshot is
/// sent again: refusals double the wait from `SNAPSHOT_RESEND_TICKS` up to
/// this (SONNY-6636).
pub const SNAPSHOT_RESEND_MAX_TICKS: u32 = 256;

pub const ConfigError = error{
    InvalidId,
    InvalidElectionTick,
    InvalidHeartbeatTick,
    HeartbeatNotLessThanElection,
    InvalidSnapshotChunkSize,
    DuplicatePeerId,
    SelfInPeers,
};

/// From `RaftNode.snapshotPoint`: the index, term and (owned) config a
/// snapshot taken at the applied index carries (SONNY-6653).
pub const SnapshotPoint = struct {
    index: u64,
    term: u64,
    voters: []u64,
    voters_outgoing: []u64,
    is_joint: bool,
    allocator: std.mem.Allocator,

    /// Borrows this point's config.
    pub fn metadata(self: *const SnapshotPoint) rpc.SnapshotMetadata {
        return .{ .index = self.index, .term = self.term, .config = .{
            .voters = self.voters,
            .voters_outgoing = self.voters_outgoing,
            .is_joint = self.is_joint,
        } };
    }

    pub fn deinit(self: *SnapshotPoint) void {
        self.allocator.free(self.voters);
        self.allocator.free(self.voters_outgoing);
        self.* = undefined;
    }
};

pub const Config = struct {
    id: u64,
    peers: []const u64,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    max_entries_per_msg: u32 = 64,
    max_bytes_per_msg: u64 = 1024 * 1024,
    max_entry_size: u64 = 1024 * 1024, // 1 MiB default; 0 = unlimited
    max_snapshot_size: u64 = 256 * 1024 * 1024, // 256 MiB default; 0 = unlimited
    snapshot_chunk_size: usize = 64 * 1024,
    snapshot_threshold: u32 = 1000,
    pre_vote: bool = true,
    /// RC-B it.2: after this many CONSECUTIVE failed candidate elections (no
    /// win and no higher-term contact — e.g. a transport-degraded candidate, or
    /// a pre-vote mutual-grant lockstep where peers grant each other's pre-votes
    /// so no rejection quorum ever forms), the candidate steps down to follower
    /// and backs off with extra randomized jitter, desynchronizing lockstep
    /// peers so one can get ahead and win. This is the timer-based complement to
    /// the quorum-rejection VoteLost demotion. Liveness-only; no safety effect.
    /// 0 disables it (pre-it.2 behaviour: a candidate re-elects forever).
    candidate_demote_threshold: u32 = 4,
    rng_seed: ?u64 = null,
    /// Leave InstallSnapshot transfers to the consumer (SONNY-6653). When a
    /// peer needs a snapshot the leader does not read it from storage inside
    /// `tick()` / `step()`; it puts a request in `Ready.snapshot_sends`, and
    /// the consumer streams the snapshot with `snapshot.streamSnapshot` after
    /// releasing its node lock. Off by default: the leader loads the whole
    /// snapshot and sends it from inside the call, as before.
    stream_snapshots: bool = false,

    pub fn validate(self: Config) ConfigError!void {
        if (self.id == 0) return ConfigError.InvalidId;
        if (self.election_tick == 0) return ConfigError.InvalidElectionTick;
        if (self.heartbeat_tick == 0) return ConfigError.InvalidHeartbeatTick;
        if (self.heartbeat_tick >= self.election_tick) return ConfigError.HeartbeatNotLessThanElection;
        // Minimum 1024 bytes prevents accidental resource exhaustion in
        // buildInstallSnapshotMessages() (SEC-0100).
        if (self.snapshot_chunk_size < 1024) return ConfigError.InvalidSnapshotChunkSize;

        // Reject self-inclusion, zero IDs, and duplicate peer IDs (SEC-0033, SEC-0085).
        for (self.peers, 0..) |peer, i| {
            if (peer == 0) return ConfigError.InvalidId;
            if (peer == self.id) return ConfigError.SelfInPeers;
            for (self.peers[0..i]) |prev| {
                if (peer == prev) return ConfigError.DuplicatePeerId;
            }
        }
    }
};

/// Observable performance metrics for the Raft node (PERF-020).
/// All counters are monotonically increasing; compute rates by diffing snapshots.
/// Safe to read concurrently — the Raft core is single-threaded, so all
/// increments happen on the same thread that calls tick/step/propose/advance.
pub const Stats = struct {
    /// Number of proposals accepted via propose().
    proposals: u64 = 0,
    /// Number of committed entries applied via advance().
    entries_applied: u64 = 0,
    /// Number of elections started (including pre-vote).
    elections_started: u64 = 0,
    /// Number of times this node became leader.
    became_leader: u64 = 0,
    /// Number of heartbeat rounds sent (leader only).
    heartbeats_sent: u64 = 0,
    /// Number of heartbeat rounds where `submitBatch` returned a partial
    /// result — some peers were not submitted to the pool and fell
    /// through to the `emptyHeartbeat` fallback. A rising value here
    /// signals pool saturation; operators should inspect the pool's
    /// `statsSnapshot().queue_depth_peak` and consider raising
    /// `Config.queue_capacity` or reducing concurrent submit pressure.
    heartbeat_partial_submits: u64 = 0,
    /// Number of snapshots installed (follower receiving from leader).
    snapshots_installed: u64 = 0,
    /// Number of InstallSnapshot transfers initiated (leader auto-driving a
    /// compacted laggard). Counts transfers, not individual chunks (SEC-0144).
    snapshots_sent: u64 = 0,
    /// Number of ticks processed.
    ticks: u64 = 0,
    /// Number of messages stepped (RPC messages processed via step()).
    messages_stepped: u64 = 0,
    /// Number of times a stuck candidate stepped down to follower via the
    /// timer-based demotion (RC-B it.2) rather than via a higher-term message.
    candidate_demotions: u64 = 0,
};

pub const RaftNode = struct {
    id: u64,
    role: st.Role,
    leader_id: ?u64,
    current_term: u64,
    voted_for: ?u64,
    is_pre_vote_election: bool,
    pending_config_index: u64, // index of the joint config entry, 0 = none
    log: raft_log.RaftLog,
    election_state: elect.ElectionState,
    replication_state: ?repl.ReplicationState,
    membership_state: mem.MembershipState,
    snapshot_state: snap.SnapshotState,
    ready_state: rd.ReadyState,
    config: Config,
    transport: ?trans.Transport,
    storage: stg.Storage,
    /// The allocator used for all internal allocations.
    ///
    /// **Thread-safety requirement (SEC-0067):** When passing a non-null
    /// `*tp.ThreadPool` to `tick()` or `step()`, parallel worker threads will
    /// allocate from this allocator concurrently (e.g. in `parallelBuildAE`).
    /// In that case, the
    /// allocator **must** be thread-safe. For example, use
    /// `std.heap.GeneralPurposeAllocator(.{ .thread_safe = true })` or
    /// `std.heap.page_allocator`. If `pool` is always null (sequential mode),
    /// any allocator is safe.
    allocator: std.mem.Allocator,
    /// Stack buffer for dynamicPeers() — avoids heap allocation for clusters
    /// with up to 16 voters (PERF-004).
    peers_stack_buf: [MAX_STACK_PEERS]u64 = undefined,
    /// Observable performance counters (PERF-020).
    perf_stats: Stats = .{},
    /// RC-B it.2: consecutive failed candidate elections (no win, no higher-term
    /// contact). Reset on becomeLeader/becomeFollower; drives the timer-based
    /// candidate demotion in tick(). See Config.candidate_demote_threshold.
    consecutive_election_failures: u32 = 0,
    /// Index of the final config entry (C_new) that removes this leader, or
    /// 0. The leader keeps leading (without counting itself, as C_new
    /// excludes it) until that entry commits, then steps down (Raft §4.2.2,
    /// SONNY-6611).
    stepdown_index: u64 = 0,

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Initialize a new RaftNode.
    ///
    /// The `allocator` must be thread-safe if thread-pool parallelism will be
    /// used (i.e. non-null `pool` parameters passed to `tick()`/`step()`). See
    /// the `allocator` field doc comment for details (SEC-0067).
    pub fn init(allocator: std.mem.Allocator, config: Config, storage_impl: stg.Storage) !RaftNode {
        try config.validate();

        // Load snapshot ONCE for both RaftLog and MembershipState init (PERF-021).
        const loaded_snap = try storage_impl.loadSnapshot();

        // Extract snapshot index/term for RaftLog.
        var snap_idx: u64 = 0;
        var snap_trm: u64 = 0;
        if (loaded_snap) |s| {
            snap_idx = s.metadata.index;
            snap_trm = s.metadata.term;
        }

        var log_inst = try raft_log.RaftLog.init(allocator, storage_impl, snap_idx, snap_trm);
        // Free log on init failure to prevent leak (SEC-0126).
        errdefer log_inst.deinit();

        // Build initial voter set: use snapshot config if available,
        // otherwise fall back to static config.peers (SEC-0073).
        var membership_inst: mem.MembershipState = undefined;
        // Track whether snapshot is in joint consensus for pending_config_index (SEC-0123).
        var snap_pending_config_index: u64 = 0;
        if (loaded_snap) |snap_val| {
            defer stg.freeSnapshot(allocator, snap_val);
            if (snap_val.metadata.config.voters.len > 0) {
                // Restore membership from snapshot config (SEC-0073).
                membership_inst = try mem.MembershipState.init(allocator, snap_val.metadata.config.voters);
                if (snap_val.metadata.config.is_joint and snap_val.metadata.config.voters_outgoing.len > 0) {
                    try membership_inst.voters_outgoing.appendSlice(allocator, snap_val.metadata.config.voters_outgoing);
                    membership_inst.is_joint = true;
                    // Joint consensus snapshot: set pending_config_index so a
                    // new leader can finalize the transition (SEC-0123).
                    snap_pending_config_index = snap_val.metadata.index;
                }
            } else {
                // Snapshot has no config — fall back to static peers.
                var all_voters = std.ArrayList(u64).empty;
                defer all_voters.deinit(allocator);
                try all_voters.append(allocator, config.id);
                try all_voters.appendSlice(allocator, config.peers);
                membership_inst = try mem.MembershipState.init(allocator, all_voters.items);
            }
        } else {
            // No snapshot — use static config.peers.
            var all_voters = std.ArrayList(u64).empty;
            defer all_voters.deinit(allocator);
            try all_voters.append(allocator, config.id);
            try all_voters.appendSlice(allocator, config.peers);
            membership_inst = try mem.MembershipState.init(allocator, all_voters.items);
        }

        // Free membership on init failure to prevent leak (SEC-0126).
        errdefer membership_inst.deinit();

        // Restore hard state from storage to prevent double-voting on
        // restart and term regression (SEC-0077).
        const restored_hs = try storage_impl.loadHardState();
        const initial_term: u64 = restored_hs.current_term;
        const initial_voted_for: ?u64 = restored_hs.voted_for;

        return .{
            .id = config.id,
            .role = .follower,
            .leader_id = null,
            .current_term = initial_term,
            .voted_for = initial_voted_for,
            .is_pre_vote_election = false,
            .pending_config_index = snap_pending_config_index,
            .log = log_inst,
            .election_state = elect.ElectionState.init(
                allocator,
                config.election_tick,
                config.heartbeat_tick,
                config.rng_seed,
            ),
            .replication_state = null,
            .membership_state = membership_inst,
            .snapshot_state = snap.SnapshotState.init(
                allocator,
                config.snapshot_threshold,
                config.snapshot_chunk_size,
                config.max_snapshot_size,
            ),
            // Use restored hard state to avoid redundant persist on first
            // advance() after restart (SEC-0131).
            .ready_state = rd.ReadyState.init(allocator, restored_hs),
            .config = config,
            .transport = null,
            .storage = storage_impl,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RaftNode) void {
        self.log.deinit();
        self.election_state.deinit();
        if (self.replication_state) |*rs| {
            rs.deinit();
            self.replication_state = null;
        }
        self.membership_state.deinit();
        self.snapshot_state.deinit();
        self.ready_state.deinit();
    }

    pub fn setTransport(self: *RaftNode, t: trans.Transport) void {
        self.transport = t;
    }

    /// Opt the calling thread into the thread-pool's per-submitter fast path
    /// (PERF-027). Raft heartbeats are submitted in bursts from the same
    /// driving thread (the caller of `tick()`); registering that thread as a
    /// dedicated submitter lets `submitBatch` bypass global-freelist CAS
    /// traffic for descriptor acquisition.
    ///
    /// Call **once** on the thread that will drive `tick()` before entering
    /// the tick loop; pair with exactly one `releaseAsSubmitter` on the same
    /// thread before the loop exits. Each call claims a fresh slot from the
    /// pool's 64-slot submitter bitmap, so do not call repeatedly — wrap the
    /// acquisition around the tick loop and reuse the returned slot for the
    /// loop's lifetime.
    ///
    /// Returns `error.NoSubmitterSlots` if the pool's 64-slot bitmap is full.
    /// Callers that cannot afford to fail may ignore the error and fall back
    /// to the default submit path (correct but slower under contention).
    pub fn registerAsSubmitter(_: *RaftNode, pool: *tp.ThreadPool) !usize {
        return pool.registerSubmitter();
    }

    /// Release a submitter slot previously acquired via `registerAsSubmitter`.
    /// Must be called on the same thread that registered the slot for the
    /// per-slot descriptor cache to drain correctly. Out-of-range `idx` is
    /// silently ignored. Safe to double-release a given `idx` — the pool's
    /// CAS on the free-mask bails on the second call rather than racing.
    pub fn releaseAsSubmitter(_: *RaftNode, pool: *tp.ThreadPool, idx: usize) void {
        pool.releaseSubmitter(idx);
    }

    /// Return the current dynamic voter set excluding self (SEC-0044).
    /// Uses membership_state.voters (which tracks config changes) instead
    /// of the static config.peers. Caller must free the returned slice.
    fn dynamicPeers(self: *RaftNode) ![]u64 {
        const voters = self.membership_state.voters.items;
        // Count peers (voters minus self).
        var count: usize = 0;
        for (voters) |v| {
            if (v != self.id) count += 1;
        }
        // Also include voters_outgoing (joint consensus) minus self and minus duplicates.
        const outgoing = self.membership_state.voters_outgoing.items;
        for (outgoing) |v| {
            if (v == self.id) continue;
            var already = false;
            for (voters) |iv| {
                if (iv == v) {
                    already = true;
                    break;
                }
            }
            if (!already) count += 1;
        }

        if (count == 0) return &.{};
        // Use stack buffer for typical cluster sizes (PERF-004).
        const result = if (count <= 16)
            self.peers_stack_buf[0..count]
        else
            try self.allocator.alloc(u64, count);

        var i: usize = 0;
        for (voters) |v| {
            if (v != self.id) {
                result[i] = v;
                i += 1;
            }
        }
        for (outgoing) |v| {
            if (v == self.id) continue;
            var already = false;
            for (voters) |iv| {
                if (iv == v) {
                    already = true;
                    break;
                }
            }
            if (!already) {
                result[i] = v;
                i += 1;
            }
        }
        return result;
    }

    /// Propose a command to the replicated log.
    /// Returns `error.NotLeader` if this node is not the current leader.
    /// On error, the caller can read `self.leader_id` to discover the
    /// current known leader (may be `null` if unknown, e.g. during election).
    pub fn propose(self: *RaftNode, data: []const u8) !void {
        if (self.role != .leader) return error.NotLeader;
        if (self.config.max_entry_size > 0 and data.len > self.config.max_entry_size)
            return error.EntryTooLarge;

        // Checked overflow: lastIndex + 1 (SEC-0071).
        const next_index = std.math.add(u64, self.log.lastIndex(), 1) catch return error.Overflow;
        const entry = rpc.Entry{
            .index = next_index,
            .term = self.current_term,
            .kind = .normal,
            .data = data,
        };
        // log.append internally duplicates data, no need to pre-copy.
        try self.log.append(&.{entry});
        self.perf_stats.proposals += 1;
    }

    pub fn proposeConfigChange(self: *RaftNode, new_peers: []const u64) !void {
        if (self.role != .leader) return error.NotLeader;

        // Validate new_peers: reject zero IDs and duplicates (SEC-0058).
        for (new_peers, 0..) |peer, i| {
            if (peer == 0) return error.InvalidData;
            for (new_peers[0..i]) |prev| {
                if (peer == prev) return error.InvalidData;
            }
        }

        // Pre-validate serialized size against max_entry_size BEFORE mutating
        // membership state (SEC-0029). Joint config format:
        //   [0x01][new_count: u32][new u64s...][old_count: u32][old u64s...]
        if (self.config.max_entry_size > 0) {
            const current_voter_count = self.membership_state.voters.items.len;
            const new_size = std.math.mul(usize, new_peers.len, 8) catch return error.EntryTooLarge;
            const old_size = std.math.mul(usize, current_voter_count, 8) catch return error.EntryTooLarge;
            const total = std.math.add(usize, 1 + 4 + 4, std.math.add(usize, new_size, old_size) catch return error.EntryTooLarge) catch return error.EntryTooLarge;
            if (total > self.config.max_entry_size) {
                return error.EntryTooLarge;
            }
        }

        // Checked overflow: lastIndex + 1 (SEC-0071).
        const cfg_index = std.math.add(u64, self.log.lastIndex(), 1) catch return error.Overflow;
        const entry = try self.membership_state.proposeChange(
            new_peers,
            self.current_term,
            cfg_index,
        );
        // Free serialized config data if log.append fails (SEC-0047).
        errdefer if (entry.data.len > 0) self.allocator.free(entry.data);
        // Roll back prepared voter list if log.append fails (SEC-0138).
        errdefer self.membership_state.rollbackProposeChange();

        try self.log.append(&.{entry});
        // Mutate membership state only AFTER log.append succeeds (SEC-0138).
        self.membership_state.commitProposeChange();
        self.pending_config_index = entry.index;
    }

    /// Process an inbound RPC message from another node.
    ///
    /// NOTE: This library validates that `from` matches the sender ID
    /// embedded in messages (e.g. candidate_id, leader_id) where applicable.
    /// However, the `from` parameter itself is supplied by the transport layer.
    /// Transport implementations MUST authenticate peers (e.g. via mTLS or
    /// HMAC-signed messages) to prevent impersonation attacks.
    pub fn step(self: *RaftNode, from: u64, msg: rpc.Message) !void {

        // Validate that the transport-provided sender matches the message's
        // internal sender field to prevent spoofed/misrouted messages.
        switch (msg) {
            .request_vote => |rv| {
                if (rv.candidate_id != from) {
                    std.log.warn("raft: sender mismatch in RequestVote: from={d} candidate_id={d}", .{ from, rv.candidate_id });
                    return;
                }
            },
            .append_entries => |ae| {
                if (ae.leader_id != from) {
                    std.log.warn("raft: sender mismatch in AppendEntries: from={d} leader_id={d}", .{ from, ae.leader_id });
                    return;
                }
            },
            .install_snapshot => |is| {
                if (is.leader_id != from) {
                    std.log.warn("raft: sender mismatch in InstallSnapshot: from={d} leader_id={d}", .{ from, is.leader_id });
                    return;
                }
            },
            // Response messages don't carry a sender ID — rely on transport.
            .request_vote_response, .append_entries_response, .install_snapshot_response => {},
        }

        self.perf_stats.messages_stepped += 1;

        switch (msg) {
            .request_vote => |rv| try self.handleRequestVote(from, rv),
            .request_vote_response => |rvr| try self.handleRequestVoteResponse(from, rvr),
            .append_entries => |ae| try self.handleAppendEntries(from, ae),
            .append_entries_response => |aer| try self.handleAppendEntriesResponse(from, aer),
            .install_snapshot => |is| try self.handleInstallSnapshot(from, is),
            .install_snapshot_response => |isr| try self.handleInstallSnapshotResponse(from, isr),
        }
    }

    /// Advance internal timers and trigger leader/follower actions.
    /// When `pool` is non-null, thread-pool parallelism is used for building
    /// AppendEntries payloads in parallel during leader heartbeats.
    pub fn tick(self: *RaftNode, pool: ?*tp.ThreadPool) !void {
        self.perf_stats.ticks += 1;
        if (self.role == .leader) {
            self.election_state.tickHeartbeat();
            if (self.election_state.heartbeatTimeoutElapsed()) {
                self.election_state.resetHeartbeatElapsed();
                try self.sendHeartbeats(pool);
            }

            // Check if joint config committed and needs phase-2.
            if (self.membership_state.is_joint and self.pending_config_index > 0 and self.log.committed >= self.pending_config_index) {
                // Checked overflow: lastIndex + 1 (SEC-0071).
                const finalize_index = std.math.add(u64, self.log.lastIndex(), 1) catch return error.Overflow;
                const final_entry = try self.membership_state.finalizeChange(
                    self.current_term,
                    finalize_index,
                );
                if (final_entry) |entry| {
                    // Free serialized config data if log.append fails (SEC-0048).
                    errdefer if (entry.data.len > 0) self.allocator.free(entry.data);
                    try self.log.append(&.{entry});
                    // Mutate membership state only AFTER log.append succeeds,
                    // preventing Raft §6 safety violation on OOM (SEC-0139).
                    self.membership_state.commitFinalizeChange();
                    self.pending_config_index = 0;
                    if (!mem.isVoter(self.membership_state.config(), self.id)) {
                        self.stepdown_index = entry.index;
                    }
                }
            }

            // A leader outside C_new steps down once C_new is committed, not
            // when it is appended: until then the remaining voters may not
            // have it, and nobody else can drive it to commit (Raft §4.2.2).
            // The heartbeat above already carried the new commit index.
            if (self.membership_state.shouldLeaderStepDown(self.id) and
                self.log.committed >= self.stepdown_index)
            {
                self.becomeFollower(self.current_term, null);
                return;
            }

            // Try to advance commit index.
            self.maybeCommit();
        } else {
            self.election_state.tickElection();
            if (self.election_state.electionTimeoutElapsed()) {
                // A node outside its own voter set (removed by a membership
                // change) must not campaign (etcd `promotable`): peers still
                // in the joint config would elect it, and its first act as
                // leader is to step down again, starving the real voters.
                if (!mem.isVoter(self.membership_state.config(), self.id)) {
                    self.election_state.resetElectionElapsed();
                    return;
                }
                // RC-B it.2: a candidate whose election just timed out made no
                // progress this round (no win → would still be a candidate; a
                // win or a higher-term message resets the counter via
                // becomeLeader/becomeFollower). After `candidate_demote_threshold`
                // consecutive such failures, step down to follower and back off
                // instead of re-campaigning. This covers the cases the
                // quorum-rejection VoteLost path cannot — a transport-degraded
                // candidate that collects no quorum at all, and a pre-vote
                // mutual-grant lockstep where no rejection quorum ever forms.
                // Liveness-only: no term/log change, no safety impact.
                if (self.role == .candidate and self.config.candidate_demote_threshold > 0) {
                    self.consecutive_election_failures +|= 1;
                    if (self.consecutive_election_failures >= self.config.candidate_demote_threshold) {
                        const failures = self.consecutive_election_failures;
                        self.becomeFollower(self.current_term, null); // resets counter
                        self.election_state.applyElectionBackoff(failures);
                        self.perf_stats.candidate_demotions += 1;
                        return;
                    }
                }
                try self.startElection(self.config.pre_vote);
            }
        }
    }

    pub fn ready(self: *RaftNode) !rd.Ready {
        // Refuse before collecting anything: a second call before advance()
        // would add the committed entries again (and could move the slice the
        // previous Ready handed out) (SONNY-6611).
        if (self.ready_state.has_pending_ready) return error.ReadyNotAdvanced;

        // Collect committed entries.
        if (self.log.committed > self.log.applied) {
            // Checked overflow: applied + 1, committed + 1 (SEC-0071).
            const slice_lo = std.math.add(u64, self.log.applied, 1) catch return error.Overflow;
            const slice_hi = std.math.add(u64, self.log.committed, 1) catch return error.Overflow;

            // Try zero-copy borrow first when range is entirely in unstable entries (PERF-003).
            if (self.log.borrowUnstable(slice_lo, slice_hi)) |borrowed| {
                try self.ready_state.addCommittedEntries(borrowed);
            } else {
                const entries = try self.log.slice(
                    slice_lo,
                    slice_hi,
                );
                // The Ready owns the slice and its data until advance(); it is
                // freed here only if handing it over failed (SEC-0117,
                // SONNY-6592).
                errdefer rpc.freeEntries(self.allocator, entries);
                try self.ready_state.addOwnedCommittedEntries(entries);
            }
        }

        // Check shouldSnapshot.
        if (self.snapshot_state.shouldSnapshot(self.log.lastIndex())) {
            self.ready_state.setShouldSnapshot(true);
        }

        return try self.ready_state.ready(self.currentHardState());
    }

    /// Convenience method that calls advancePersist() then advanceApply().
    /// It releases `Ready.messages` unsent: a consumer with peers calls
    /// advancePersist(), sends `Ready.messages`, then advanceApply().
    pub fn advance(self: *RaftNode) !void {
        try self.advancePersist();
        try self.advanceApply();
    }

    /// Phase 1: persist hard state and unstable entries to storage.
    /// Only after this returns may the consumer send `Ready.messages`; they
    /// announce the vote, term and entries just persisted (SONNY-6592). If
    /// it fails, do not send them.
    pub fn advancePersist(self: *RaftNode) !void {
        const hs = self.currentHardState();

        // Persist a follower-installed snapshot to durable storage BEFORE any
        // entries, so the backing store's snapshot_index matches the in-memory
        // RaftLog that handleInstallSnapshot already restored. Without this the
        // store keeps stale pre-snapshot entries and later AppendEntries land at
        // the wrong offset, desyncing lastIndex() from term()/entries()
        // (SEC-0144). The leader-initiated path persists in reportSnapshot();
        // this is the follower-side analogue. saveSnapshot advances the store's
        // snapshot_index so the subsequent appendEntries truncates the stale
        // tail correctly.
        if (self.ready_state.ready_snapshot) |ready_snap| {
            try self.storage.saveSnapshot(ready_snap);
        }

        // Persist hard state if changed.
        if (!hs.eql(self.ready_state.prev_hard_state)) {
            try self.storage.saveHardState(hs);
        }

        // Persist unstable entries to storage.
        const unstable = self.log.unstableSlice();
        if (unstable.len > 0) {
            try self.storage.appendEntries(unstable);
        }
    }

    /// Phase 2: apply committed entries, update membership, stabilize log.
    /// Must be called after advancePersist() completes (PERF-006).
    pub fn advanceApply(self: *RaftNode) !void {
        const hs = self.currentHardState();

        // Apply committed config entries to MembershipState (SEC-0073).
        // This ensures followers (and leaders after restart) update their
        // voter sets when config change entries are committed.
        // Reuse entries already collected by ready() instead of slicing
        // the log a second time (PERF-002).
        {
            var had_config = false;
            for (self.ready_state.pending_committed_entries.items) |entry| {
                if (entry.kind == .config and entry.data.len > 0) {
                    try self.membership_state.applyConfig(entry);
                    had_config = true;
                    // Track joint config index so a new leader can finalize
                    // the membership change if the original leader crashes
                    // after the joint entry commits (SEC-0120).
                    if (self.membership_state.is_joint) {
                        self.pending_config_index = entry.index;
                    } else {
                        self.pending_config_index = 0;
                    }
                }
            }
            // If config changed and we are leader, sync replication_state
            // peers to match the new voter set (SEC-0075).
            if (had_config and self.role == .leader) {
                try self.syncReplicationPeers();
            }
        }

        // Mark committed entries as applied.
        if (self.log.committed > self.log.applied) {
            self.perf_stats.entries_applied += self.ready_state.pending_committed_entries.items.len;
            try self.log.appliedTo(self.log.committed);
        }

        // Advance ready state.
        self.ready_state.advance(hs);

        // Stabilize log entries.
        const unstable = self.log.unstableSlice();
        if (unstable.len > 0) {
            const last = unstable[unstable.len - 1];
            try self.log.stableTo(last.index, last.term);
            // The leader's own entries now count toward commit; without
            // this a single-node cluster would wait for another tick.
            if (self.role == .leader) self.maybeCommit();
        }
    }

    /// Where a snapshot of the state machine taken now must stand: the
    /// applied index, its term and the current configuration (SONNY-6653).
    /// Call under the node lock, then build the snapshot of the state at
    /// that index, stage it with `Storage.stageSnapshot` (off the lock) and
    /// commit it with `commitSnapshot` (under the lock).
    pub fn snapshotPoint(self: *RaftNode) !SnapshotPoint {
        const index = self.log.applied;
        const term = self.log.term(index) catch 0;
        const cfg = self.membership_state.config();
        const voters = try self.allocator.dupe(u64, cfg.voters);
        errdefer self.allocator.free(voters);
        const outgoing = try self.allocator.dupe(u64, cfg.voters_outgoing);
        return .{
            .index = index,
            .term = term,
            .voters = voters,
            .voters_outgoing = outgoing,
            .is_joint = cfg.is_joint,
            .allocator = self.allocator,
        };
    }

    /// Commit a snapshot staged off the lock (SONNY-6653): the staged
    /// counterpart of `reportSnapshot`. Call under the node lock. With
    /// `FileStorage` this does only small writes (metadata, renames,
    /// directory syncs) and the log compaction, whatever the snapshot size.
    ///
    /// error.SnapshotOutOfDate: the log is already compacted to or past the
    /// staged index (a newer snapshot was committed or installed meanwhile);
    /// error.SnapshotNotApplied: the index is past the applied index;
    /// error.SnapshotTermMismatch: the log's term at that index differs.
    /// In each case nothing changed; release the staged snapshot.
    pub fn commitSnapshot(self: *RaftNode, staged: *stg.StagedSnapshot) !void {
        const index = staged.metadata.index;
        if (index <= self.log.snapshot_index) return error.SnapshotOutOfDate;
        if (index > self.log.applied) return error.SnapshotNotApplied;
        const term = self.log.term(index) catch return error.SnapshotOutOfDate;
        if (term != staged.metadata.term) return error.SnapshotTermMismatch;

        try self.storage.commitStagedSnapshot(staged);
        self.snapshot_state.snapshot_index = index;
        self.snapshot_state.snapshot_term = term;
        try self.log.compact(index);
    }

    /// Write the snapshot and compact the log in one call, under the node
    /// lock. The data write (and with `FileStorage` its fsync) happens
    /// inside: for large snapshots use `snapshotPoint`,
    /// `Storage.stageSnapshot` and `commitSnapshot` (SONNY-6653).
    pub fn reportSnapshot(self: *RaftNode, data: []const u8) !void {
        const last_applied = self.log.applied;
        const applied_term = self.log.term(last_applied) catch 0;
        const cfg = self.membership_state.config();

        const snapshot_val = self.snapshot_state.createLocalSnapshot(
            data,
            last_applied,
            applied_term,
            cfg,
        );

        try self.storage.saveSnapshot(snapshot_val);
        try self.log.compact(last_applied);
    }

    // -----------------------------------------------------------------------
    // Internal methods
    // -----------------------------------------------------------------------

    fn becomeFollower(self: *RaftNode, term: u64, leader_id: ?u64) void {
        if (term != self.current_term) {
            self.voted_for = null;
        }
        self.current_term = term;
        self.role = .follower;
        self.leader_id = leader_id;
        self.is_pre_vote_election = false;
        self.consecutive_election_failures = 0;
        self.stepdown_index = 0;
        self.election_state.resetElectionElapsed();
        self.election_state.resetElectionTimeout();

        if (self.replication_state) |*rs| {
            rs.deinit();
            self.replication_state = null;
        }
    }

    fn becomeLeader(self: *RaftNode) !void {
        // Initialize replication_state with dynamic voters (SEC-0044).
        // All fallible operations run BEFORE committing the role change so
        // that OOM cannot leave us in a zombie-leader state (SEC-0103).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 16) self.allocator.free(peers);
        const rs = try repl.ReplicationState.init(
            self.allocator,
            peers,
            self.log.lastIndex(),
        );

        // Append no-op entry (critical for correctness, section 2.7).
        // Checked overflow: lastIndex + 1 (SEC-0071).
        const noop_index = std.math.add(u64, self.log.lastIndex(), 1) catch return error.Overflow;
        const noop = rpc.Entry{
            .index = noop_index,
            .term = self.current_term,
            .kind = .noop,
            .data = &.{},
        };
        self.log.append(&.{noop}) catch |err| {
            // Clean up replication state on noop append failure (SEC-0103).
            var rs_mut = rs;
            rs_mut.deinit();
            return err;
        };

        // All fallible operations succeeded — commit the state transition.
        self.role = .leader;
        self.leader_id = self.id;
        self.is_pre_vote_election = false;
        self.consecutive_election_failures = 0;
        self.replication_state = rs;
        self.election_state.resetHeartbeatElapsed();
        self.perf_stats.became_leader += 1;
    }

    /// Synchronize replication_state.peers with the current voter set.
    /// Adds new voters, removes departed voters, preserves existing progress (SEC-0075).
    fn syncReplicationPeers(self: *RaftNode) !void {
        var rs = &(self.replication_state orelse return);
        const peers = try self.dynamicPeers();
        defer if (peers.len > 16) self.allocator.free(peers);

        // Add any new peers not already tracked.
        const next_index = std.math.add(u64, self.log.lastIndex(), 1) catch self.log.lastIndex();
        for (peers) |pid| {
            if (rs.peers.get(pid) == null) {
                try rs.peers.put(pid, .{
                    .next_index = next_index,
                    .match_index = 0,
                });
            }
        }

        // Remove peers that are no longer in the voter set.
        // Build a list of keys to remove (can't modify during iteration).
        var to_remove = std.ArrayList(u64).empty;
        defer to_remove.deinit(self.allocator);
        var it = rs.peers.iterator();
        while (it.next()) |kv| {
            var found = false;
            for (peers) |pid| {
                if (kv.key_ptr.* == pid) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                try to_remove.append(self.allocator, kv.key_ptr.*);
            }
        }
        for (to_remove.items) |key| {
            _ = rs.peers.remove(key);
        }
    }

    fn startElection(self: *RaftNode, pre_vote: bool) !void {
        // One span per election fan-out (client-side). The whole
        // RequestVote broadcast is the unit of work. `target=0` is the
        // broadcast sentinel (Raft node IDs start at 1, so 0 cannot
        // collide with a real peer ID — see Config.validate's
        // InvalidId rejection of id == 0). Telemetry is non-fatal.
        var __obs_span = observability.startRpcClientSpan("RequestVote", 0);
        defer __obs_span.end(null);
        errdefer observability.endRpcClientSpan(__obs_span, false, -1);

        if (pre_vote) {
            self.is_pre_vote_election = true;
            // Don't increment term or set voted_for for pre-vote.
        } else {
            self.current_term = std.math.add(u64, self.current_term, 1) catch return error.Overflow;
            self.voted_for = self.id;
            self.is_pre_vote_election = false;
        }

        self.role = .candidate;
        self.perf_stats.elections_started += 1;
        self.election_state.resetElectionElapsed();
        self.election_state.resetElectionTimeout();

        // Reset votes, record own vote.
        self.election_state.resetVotes();
        try self.election_state.recordVote(self.id, true);

        // Use dynamic voters for election (SEC-0044).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 16) self.allocator.free(peers);

        // Single-node cluster: become leader immediately.
        if (peers.len == 0) {
            if (pre_vote) {
                // Pre-vote succeeded (self-only), start real election.
                try self.startElection(false);
            } else {
                try self.becomeLeader();
            }
            return;
        }

        // Build and send RequestVote to all peers.
        const request_term = if (pre_vote) std.math.add(u64, self.current_term, 1) catch return error.Overflow else self.current_term;
        const msg = rpc.Message{ .request_vote = .{
            .term = request_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
            .pre_vote = pre_vote,
        } };

        for (peers) |peer| {
            self.sendAfterPersist(peer, msg);
        }
        observability.endRpcClientSpan(__obs_span, true, 0);
    }

    fn handleRequestVote(self: *RaftNode, from: u64, msg: rpc.RequestVote) !void {
        // One span per inbound RequestVote (server-side). The whole RPC
        // is the unit of work — sub-helpers (sendMsg, log lookups) are
        // not separately spanned. Telemetry is non-fatal: any failure
        // inside the seam is `catch {}`-discarded; consensus continues.
        // The deferred `end(null)` always runs; the trailing
        // `endRpcServerSpan(true, 0)` annotates the success status. On
        // error returns the `errdefer` annotates the failure status.
        var __obs_span = observability.startRpcServerSpan("RequestVote", from);
        defer __obs_span.end(null);
        errdefer observability.endRpcServerSpan(__obs_span, false, -1);

        // Reject messages from node ID 0 — invalid sender (SEC-0135).
        if (msg.candidate_id == 0) return;

        // If msg.term > current_term and not a pre-vote, step down.
        if (msg.term > self.current_term and !msg.pre_vote) {
            self.becomeFollower(msg.term, null);
        }

        // Reject requests from non-member nodes for symmetry with
        // response handlers (SEC-0134).
        const cfg = self.membership_state.config();
        if (!mem.isVoter(cfg, msg.candidate_id)) {
            std.log.warn("raft: RequestVote from non-member {d}, rejecting", .{msg.candidate_id});
            self.sendAfterPersist(msg.candidate_id, rpc.Message{ .request_vote_response = .{
                .term = self.current_term,
                .vote_granted = false,
                .pre_vote = msg.pre_vote,
            } });
            return;
        }

        // Leaders must NOT grant pre-votes to prevent disruptive re-elections
        // from partitioned nodes (Raft §9.6, SEC-0061).
        // Followers/candidates with a known leader also reject pre-votes
        // for same-term requests to prevent disruption (SEC-0133).
        if (msg.pre_vote) {
            if (self.role == .leader) {
                self.sendAfterPersist(msg.candidate_id, rpc.Message{ .request_vote_response = .{
                    .term = self.current_term,
                    .vote_granted = false,
                    .pre_vote = true,
                } });
                return;
            }
            if (self.leader_id != null and msg.term <= self.current_term) {
                self.sendAfterPersist(msg.candidate_id, rpc.Message{ .request_vote_response = .{
                    .term = self.current_term,
                    .vote_granted = false,
                    .pre_vote = true,
                } });
                return;
            }
        }

        const grant = elect.shouldGrantVote(
            self.voted_for,
            msg.candidate_id,
            msg.last_log_index,
            msg.last_log_term,
            self.log.lastIndex(),
            self.log.lastTerm(),
            msg.pre_vote,
        );

        // Apply term check: only grant if msg.term >= current_term.
        const should_grant = grant and msg.term >= self.current_term;

        if (should_grant and !msg.pre_vote) {
            self.voted_for = msg.candidate_id;
            self.election_state.resetElectionElapsed();
            self.election_state.resetElectionTimeout();
        }

        const response = rpc.Message{ .request_vote_response = .{
            .term = self.current_term,
            .vote_granted = should_grant,
            .pre_vote = msg.pre_vote,
        } };
        self.sendAfterPersist(msg.candidate_id, response);
        observability.endRpcServerSpan(__obs_span, true, 0);
    }

    fn handleRequestVoteResponse(self: *RaftNode, from: u64, msg: rpc.RequestVoteResponse) !void {
        if (self.role != .candidate) return;

        // Ignore if pre_vote flag doesn't match our election type.
        if (msg.pre_vote != self.is_pre_vote_election) return;

        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, null);
            return;
        }

        // Reject stale-term responses to prevent phantom votes from
        // previous elections counting toward current quorum (SEC-0119).
        if (msg.term < self.current_term) return;

        // Validate sender is a known voter before recording (SEC-0083).
        const cfg = self.membership_state.config();
        if (!mem.isVoter(cfg, from)) {
            std.log.warn("raft: RequestVoteResponse from non-voter {d}, ignoring", .{from});
            return;
        }

        try self.election_state.recordVote(from, msg.vote_granted);

        // Check quorum (reuse cfg from voter validation above).
        if (self.election_state.hasJointQuorum(cfg.voters, cfg.voters_outgoing)) {
            if (self.is_pre_vote_election) {
                try self.startElection(false);
            } else {
                try self.becomeLeader();
            }
            return;
        }

        // VoteLost: a quorum has REJECTED this (pre-)election, so it can never
        // be won at the current term. Step down to follower immediately rather
        // than burning the whole election timeout still campaigning (etcd's
        // `case quorum.VoteLost: r.becomeFollower(r.Term, None)`).
        //
        // This is the consensus-layer half of the k8s no-leader all-candidate
        // livelock fix: a quorum-denied candidate (e.g. a node frozen behind
        // the cluster under pre-vote) demotes and resets its randomized
        // election timer, so it can grant a competitor's vote instead of
        // perpetually self-voting in a split. becomeFollower keeps the current
        // term (a denied real election does not advance it; pre-vote never
        // incremented it), so this changes only liveness, never safety
        // (docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md
        // RC-B).
        if (self.election_state.hasJointRejectQuorum(cfg.voters, cfg.voters_outgoing)) {
            self.becomeFollower(self.current_term, null);
        }
    }

    fn handleAppendEntries(self: *RaftNode, from: u64, msg: rpc.AppendEntries) !void {
        // One span per inbound AppendEntries batch (server-side). Per
        // the Raft paper, the whole batch is one logical replication
        // operation — we do NOT span individual log entries (would be
        // too high-volume). Telemetry is non-fatal.
        var __obs_span = observability.startRpcServerSpan("AppendEntries", from);
        defer __obs_span.end(null);
        errdefer observability.endRpcServerSpan(__obs_span, false, -1);

        // Reject messages from node ID 0 — invalid sender (SEC-0146).
        if (msg.leader_id == 0) return;

        // Phase 1: Term validation.
        if (msg.term < self.current_term) {
            self.rejectAppendEntries(msg.leader_id);
            return;
        }

        // msg.term >= current_term: accept leader.
        self.becomeFollower(msg.term, msg.leader_id);

        // QA-023b: Log a warning if the sender is not a known voter.
        // Per the Raft paper, followers MUST accept AppendEntries from any
        // leader with a valid term, so this is observability-only — we do
        // NOT reject the message.
        if (!mem.isVoter(self.membership_state.config(), msg.leader_id)) {
            std.log.warn("raft: AppendEntries from non-member {d}, processing anyway (valid term)", .{msg.leader_id});
        }

        self.election_state.resetElectionElapsed();

        // Phase 2: Message validation (entry counts, sizes, continuity).
        if (!self.validateAppendEntriesPayload(msg)) return;

        // Phase 3: Log consistency check and entry application.
        try self.applyAppendEntries(msg);
        observability.endRpcServerSpan(__obs_span, true, 0);
    }

    /// Send a rejection AppendEntries response to the given leader (QA-002).
    /// Used for stale-term, malformed, or invalid AppendEntries messages.
    fn rejectAppendEntries(self: *RaftNode, leader_id: u64) void {
        self.sendAfterPersist(leader_id, rpc.Message{ .append_entries_response = .{
            .term = self.current_term,
            .success = false,
            .match_index = 0,
            .conflict_index = 0,
            .conflict_term = 0,
        } });
    }

    /// Validate AppendEntries payload: entry count, individual/aggregate
    /// sizes, and index continuity (SEC-0082, SEC-0127, SEC-0128, SEC-0137).
    /// Returns true if valid, false if rejected (rejection response already sent).
    fn validateAppendEntriesPayload(self: *RaftNode, msg: rpc.AppendEntries) bool {
        // Reject if entry count exceeds max_entries_per_msg (SEC-0082).
        if (msg.entries.len > self.config.max_entries_per_msg) {
            self.rejectAppendEntries(msg.leader_id);
            return false;
        }

        // Validate entry sizes and aggregate message size (SEC-0127).
        // Aggregate check runs even when max_entry_size==0 (unlimited individual
        // entries) to enforce max_bytes_per_msg.
        {
            var total_bytes: u64 = 0;
            for (msg.entries) |entry| {
                if (self.config.max_entry_size > 0 and entry.data.len > self.config.max_entry_size) {
                    self.rejectAppendEntries(msg.leader_id);
                    return false;
                }
                total_bytes = std.math.add(u64, total_bytes, entry.data.len) catch {
                    self.rejectAppendEntries(msg.leader_id);
                    return false;
                };
            }
            if (total_bytes > self.config.max_bytes_per_msg) {
                self.rejectAppendEntries(msg.leader_id);
                return false;
            }
        }

        // Validate entry index continuity: entries must be sequential
        // starting at prev_log_index + 1 (SEC-0128).
        if (msg.entries.len > 0) {
            const expected_first = std.math.add(u64, msg.prev_log_index, 1) catch {
                self.rejectAppendEntries(msg.leader_id);
                return false;
            };
            for (msg.entries, 0..) |entry, i| {
                const expected_index = std.math.add(u64, expected_first, i) catch {
                    // Overflow means prev_log_index + 1 + i > u64::MAX — reject
                    // the entire batch as malformed (SEC-0137).
                    self.rejectAppendEntries(msg.leader_id);
                    return false;
                };
                if (entry.index != expected_index) {
                    self.rejectAppendEntries(msg.leader_id);
                    return false;
                }
            }
        }

        return true;
    }

    /// Attempt to append entries to the log and send the appropriate response
    /// (success with match_index, or failure with conflict info).
    fn applyAppendEntries(self: *RaftNode, msg: rpc.AppendEntries) !void {
        const result = try self.log.maybeAppend(
            msg.prev_log_index,
            msg.prev_log_term,
            msg.leader_commit,
            msg.entries,
        );

        if (result) |match_index| {
            self.sendAfterPersist(msg.leader_id, rpc.Message{ .append_entries_response = .{
                .term = self.current_term,
                .success = true,
                .match_index = match_index,
                .conflict_index = 0,
                .conflict_term = 0,
            } });
        } else {
            const conflict = self.log.findConflict(msg.prev_log_index);
            self.sendAfterPersist(msg.leader_id, rpc.Message{
                .append_entries_response = .{
                    .term = self.current_term,
                    .success = false,
                    // On a rejection, match_index carries the rejected
                    // prev_log_index so the leader can drop stale rejections
                    // (SONNY-6611).
                    .match_index = msg.prev_log_index,
                    .conflict_index = conflict.index,
                    .conflict_term = conflict.term_val,
                },
            });
        }
    }

    fn handleAppendEntriesResponse(self: *RaftNode, from: u64, msg: rpc.AppendEntriesResponse) !void {
        if (self.role != .leader) return;

        // Validate sender is a known peer BEFORE processing term (SEC-0056).
        var rs = &(self.replication_state orelse return);
        const progress = rs.peers.getPtr(from) orelse return;

        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, null);
            return;
        }

        // Reject stale-term responses to prevent incorrect peer progress
        // updates from previous terms (SEC-0119).
        if (msg.term < self.current_term) return;

        if (msg.success) {
            _ = progress.maybeUpdate(msg.match_index);
            self.maybeCommit();
        } else {
            // A rejection carries the rejected prev_log_index in match_index
            // and the follower's conflict hint (SONNY-6611).
            const hint = repl.backtrackHint(&self.log, msg.match_index, msg.conflict_index, msg.conflict_term);
            if (progress.maybeDecrTo(msg.match_index, hint, msg.conflict_term)) {
                // Send retry AppendEntries with updated next_index.
                const ae_msg = rs.buildAppendEntries(
                    from,
                    &self.log,
                    self.current_term,
                    self.id,
                    self.log.committed,
                    self.config.max_entries_per_msg,
                    self.config.max_bytes_per_msg,
                ) catch return;

                if (ae_msg) |m| {
                    self.sendMsg(from, m);
                    // Free the entries slice allocated by buildAppendEntries (SEC-0140).
                    self.freeAppendEntriesPayload(m);
                } else {
                    // The backed-off next_index now points into the compacted
                    // region — the peer can only catch up via InstallSnapshot.
                    // This also re-triggers the snapshot if a follower acked a
                    // prior snapshot then crashed and lost it: its next rejection
                    // lands here (SEC-0144).
                    self.escalateOrHeartbeat(rs, from);
                }
            }
        }
    }

    fn handleInstallSnapshot(self: *RaftNode, from: u64, msg: rpc.InstallSnapshot) !void {
        // One span per inbound InstallSnapshot chunk (server-side).
        // Snapshot transfer is chunked, so each `handleInstallSnapshot`
        // call is one chunk — we do NOT try to span the whole multi-RPC
        // transfer here (the receiver doesn't see the boundaries until
        // `done=true`). Per-chunk spans give the right granularity for
        // exporters to correlate via the same trace context.
        var __obs_span = observability.startRpcServerSpan("InstallSnapshot", from);
        defer __obs_span.end(null);
        errdefer observability.endRpcServerSpan(__obs_span, false, -1);

        // Reject messages from node ID 0 — invalid sender (SEC-0135).
        if (msg.leader_id == 0) return;

        // Reject requests from non-member nodes (SEC-0134).
        if (!mem.isVoter(self.membership_state.config(), msg.leader_id)) {
            std.log.warn("raft: InstallSnapshot from non-member {d}, ignoring", .{msg.leader_id});
            return;
        }

        if (msg.term < self.current_term) {
            self.sendAfterPersist(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = false,
            } });
            return;
        }

        self.becomeFollower(msg.term, msg.leader_id);
        self.election_state.resetElectionElapsed();

        // Already holds this snapshot or a newer one (a re-send after a lost
        // ack, or one that arrived after a restart): that is an install, not
        // a refusal. Acknowledge it with the index held so the leader
        // advances, instead of answering StaleSnapshot (which the leader
        // counted as a refusal and backed off on) or installing it again
        // (SONNY-6652). Answer the first and the last chunk only; the ones
        // in between change nothing. The ack goes out after the next
        // advancePersist(), so a snapshot held only in memory is durable
        // first.
        const held = @max(self.log.snapshot_index, self.snapshot_state.snapshot_index);
        if (held > 0 and msg.last_included_index <= held) {
            if (msg.offset == 0 or msg.done) {
                self.sendAfterPersist(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                    .term = self.current_term,
                    .success = true,
                    .last_index = held,
                } });
            }
            observability.endRpcServerSpan(__obs_span, true, 0);
            return;
        }

        const result = self.snapshot_state.receiveChunk(msg) catch |err| {
            std.log.warn("InstallSnapshot from leader {d} failed: {s}", .{
                msg.leader_id, @errorName(err),
            });
            // A chunk out of order means one before it was lost in transit:
            // tell the leader where to resume rather than refusing the
            // snapshot (SONNY-6652).
            const gap = err == snap.SnapshotError.UnexpectedOffset;
            self.sendAfterPersist(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = false,
                .last_index = if (gap) msg.last_included_index else 0,
                .next_offset = if (gap) self.snapshot_state.resumeOffset(msg.last_included_index, msg.last_included_term) else 0,
            } });
            return;
        };

        if (result) |snapshot_val| {
            // Free snapshot data if any fallible operation below fails,
            // preventing up to 256 MiB leak (SEC-0122).
            errdefer stg.freeSnapshot(self.allocator, snapshot_val);

            // Restore log state from the snapshot so committed/applied/
            // snapshot_index track the snapshot point (SEC-0090).
            try self.log.restore(snapshot_val);

            // Apply snapshot config to MembershipState using the centralized
            // validation path (SEC-0073, SEC-0099).
            if (snapshot_val.metadata.config.voters.len > 0) {
                try self.membership_state.applySnapshotConfig(snapshot_val.metadata.config);
            }
            // If snapshot is in joint consensus, set pending_config_index
            // so the leader can finalize the transition (SEC-0123).
            if (snapshot_val.metadata.config.is_joint) {
                self.pending_config_index = snapshot_val.metadata.index;
            } else {
                // Snapshot supersedes any in-flight config change (SEC-0112).
                self.pending_config_index = 0;
            }
            // Transfer ownership to ready_state, which frees it at the
            // advance() after the Ready that delivers it (SONNY-6611); the
            // errdefer will not fire on normal return since setSnapshot is
            // infallible.
            self.ready_state.setSnapshot(snapshot_val);
            self.perf_stats.snapshots_installed += 1;
            self.sendAfterPersist(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = true,
                .last_index = snapshot_val.metadata.index,
            } });
        }
        observability.endRpcServerSpan(__obs_span, true, 0);
    }

    fn handleInstallSnapshotResponse(self: *RaftNode, from: u64, msg: rpc.InstallSnapshotResponse) !void {
        if (self.role != .leader) return;

        // Validate sender is a known peer before processing (SEC-0052).
        var rs = &(self.replication_state orelse return);
        const progress = rs.peers.getPtr(from) orelse {
            std.log.warn("raft: InstallSnapshotResponse from unknown peer {d}", .{from});
            return;
        };

        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, null);
            return;
        }

        // Reject stale-term responses to prevent incorrect peer progress
        // updates from previous terms (SEC-0119).
        if (msg.term < self.current_term) return;

        // Update peer progress on successful snapshot install (SEC-0074).
        if (msg.success) {
            // The follower reports the snapshot index it holds (SONNY-6652):
            // the one sent, or a newer one it already had. Every snapshot
            // holds only committed entries, which this leader's log holds
            // too (Leader Completeness), so that index matches; it is capped
            // at lastIndex all the same. A follower on raft <= 0.2.4 reports
            // nothing: use log.snapshot_index (persisted) rather than
            // snapshot_state.snapshot_index, which may have advanced since
            // the snapshot was sent (SEC-0113).
            //
            // maybeUpdate is forward-only, so a late/duplicate ack can never
            // regress next_index (SEC-0113); clearing pending lets the driver
            // resume normal AppendEntries replication for forward progress.
            const acked = if (msg.last_index != 0) @min(msg.last_index, self.log.lastIndex()) else self.log.snapshot_index;
            _ = progress.maybeUpdate(acked);
            // An ack of an older snapshot than the one in flight leaves it
            // in flight.
            if (msg.last_index == 0 or msg.last_index >= progress.pending_snapshot_index) {
                progress.pending_snapshot_index = 0;
                progress.pending_snapshot_ticks = 0;
                progress.snapshot_refusals = 0;
                progress.snapshot_resume_offset = 0;
            }
            self.maybeCommit();
        } else if (progress.pending_snapshot_index != 0 and msg.last_index != 0 and
            msg.last_index == progress.pending_snapshot_index)
        {
            // A chunk of the snapshot in flight was lost in transit and the
            // follower could not place the ones after it. Not a refusal: no
            // back-off, and the next send (on the normal retry interval)
            // resumes at the offset the follower expects (SONNY-6652; before,
            // each of those chunks counted as a refusal and the snapshot was
            // sent again from offset 0).
            progress.snapshot_resume_offset = msg.next_offset;
        } else if (progress.pending_snapshot_index != 0) {
            // Snapshot refused (a digest the follower cannot match, a state
            // machine that cannot take it). Sending it again at once is
            // refused the same way: it stays in flight and the next send
            // waits twice as long as the last, up to
            // SNAPSHOT_RESEND_MAX_TICKS (SONNY-6636; it was re-sent every
            // heartbeat round, SEC-0144).
            progress.snapshot_refusals +|= 1;
            progress.pending_snapshot_ticks = 0;
            progress.snapshot_resume_offset = 0;
        }
    }

    /// Build and send AppendEntries heartbeats to all peers.
    /// When `pool` is non-null, AppendEntries payloads for each peer
    /// are constructed in parallel using thread-pool submitBatch/WaitGroup.
    fn sendHeartbeats(self: *RaftNode, pool: ?*tp.ThreadPool) !void {
        // One span per heartbeat broadcast (client-side). Per the
        // constraint "Span the entire AppendEntries batch as one
        // operation", we wrap the full fan-out (parallel + sequential
        // paths) in a single span rather than emitting one span per
        // peer — that would be too high-volume at heartbeat cadence.
        // `target=0` is the broadcast sentinel (Raft IDs start at 1).
        var __obs_span = observability.startRpcClientSpan("AppendEntries", 0);
        defer __obs_span.end(null);
        errdefer observability.endRpcClientSpan(__obs_span, false, -1);

        var rs = &(self.replication_state orelse return);
        self.perf_stats.heartbeats_sent += 1;

        // Use dynamic voters instead of static config.peers (SEC-0044).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 16) self.allocator.free(peers);

        if (pool) |p| {
            // Parallel path: submit one task per peer, wait for all, then send.
            // Results are written into a shared array (PERF-012).
            // Stack-allocated; safe because wg.wait() blocks until all tasks complete.
            var inputs_stack: [MAX_STACK_PEERS]BuildAEInput = undefined;
            var results_stack: [MAX_STACK_PEERS]?rpc.Message = undefined;
            var batch_stack: [MAX_STACK_PEERS]tp.BatchTask = undefined;

            const inputs = if (peers.len <= 16)
                inputs_stack[0..peers.len]
            else
                try self.allocator.alloc(BuildAEInput, peers.len);
            defer if (peers.len > 16) self.allocator.free(inputs);

            const results = if (peers.len <= 16)
                results_stack[0..peers.len]
            else
                try self.allocator.alloc(?rpc.Message, peers.len);
            defer if (peers.len > 16) self.allocator.free(results);

            const batch = if (peers.len <= 16)
                batch_stack[0..peers.len]
            else
                try self.allocator.alloc(tp.BatchTask, peers.len);
            defer if (peers.len > 16) self.allocator.free(batch);

            for (peers, 0..) |peer, i| {
                results[i] = null;
                inputs[i] = .{
                    .rs = rs,
                    .peer = peer,
                    .log = &self.log,
                    .current_term = self.current_term,
                    .leader_id = self.id,
                    .committed = self.log.committed,
                    .max_entries_per_msg = self.config.max_entries_per_msg,
                    .max_bytes_per_msg = self.config.max_bytes_per_msg,
                    .out = &results[i],
                };
                batch[i] = .{ .task_fn = &parallelBuildAE, .ctx = @ptrCast(&inputs[i]) };
            }

            var wg = p.waitGroup();
            if (p.submitBatch(batch, .{ .wait_group = &wg })) |result| {
                // Partial-submit visibility: thread-pool accepted `result.submitted`
                // peers and silently dropped the rest. Track the count so operators
                // can observe pool saturation. Peers that weren't submitted keep
                // `results[i] = null` and fall through to `emptyHeartbeat` in the
                // send loop below — valid Raft, but an operator-visible signal.
                if (result.submitted < batch.len) {
                    self.perf_stats.heartbeat_partial_submits += 1;
                }
            } else |_| {
                // Queue full / total failure: fall back to sequential build.
                self.perf_stats.heartbeat_partial_submits += 1;
                for (peers, 0..) |peer, i| {
                    results[i] = rs.buildAppendEntries(
                        peer,
                        &self.log,
                        self.current_term,
                        self.id,
                        self.log.committed,
                        self.config.max_entries_per_msg,
                        self.config.max_bytes_per_msg,
                    ) catch null;
                }
            }
            wg.wait();

            // Send results. The parallel path only BUILDS AppendEntries on the
            // pool (parallelBuildAE is snapshot-free); snapshot escalation runs
            // here, sequentially on the main thread, since snapshots are rare
            // and must not be fanned across the pool (SEC-0144).
            for (peers, 0..) |peer, i| {
                if (results[i]) |m| {
                    self.sendMsg(peer, m);
                    // Free the entries slice allocated by buildAppendEntries (SEC-0140).
                    self.freeAppendEntriesPayload(m);
                } else {
                    self.escalateOrHeartbeat(rs, peer);
                }
            }
        } else {
            // Sequential path: build and send one at a time.
            for (peers) |peer| {
                const msg = rs.buildAppendEntries(
                    peer,
                    &self.log,
                    self.current_term,
                    self.id,
                    self.log.committed,
                    self.config.max_entries_per_msg,
                    self.config.max_bytes_per_msg,
                ) catch null;

                if (msg) |m| {
                    self.sendMsg(peer, m);
                    // Free the entries slice allocated by buildAppendEntries (SEC-0140).
                    self.freeAppendEntriesPayload(m);
                } else {
                    // null means either a compaction gap (escalate to
                    // InstallSnapshot) or a transient build error (heartbeat) —
                    // escalateOrHeartbeat disambiguates (SEC-0144).
                    self.escalateOrHeartbeat(rs, peer);
                }
            }
        }
        observability.endRpcClientSpan(__obs_span, true, 0);
    }

    /// Input for parallel AppendEntries construction.
    /// Results are written to `out` rather than returned through the task
    /// (rpc.Message is too large to return by value safely across threads).
    const BuildAEInput = struct {
        rs: *repl.ReplicationState,
        peer: u64,
        log: *raft_log.RaftLog,
        current_term: u64,
        leader_id: u64,
        committed: u64,
        max_entries_per_msg: u32,
        max_bytes_per_msg: u64,
        out: *?rpc.Message,
    };

    fn parallelBuildAE(ctx_ptr: *anyopaque, _: *tp.TaskContext) void {
        const input: *BuildAEInput = @ptrCast(@alignCast(ctx_ptr));
        input.out.* = input.rs.buildAppendEntries(
            input.peer,
            input.log,
            input.current_term,
            input.leader_id,
            input.committed,
            input.max_entries_per_msg,
            input.max_bytes_per_msg,
        ) catch null;
    }

    /// Decide what to send a peer when `buildAppendEntries` yielded null.
    ///
    /// `buildAppendEntries` returns null in two distinct cases:
    ///   1. The peer's needed entries were compacted (next_index <=
    ///      snapshot_index) — it can only be caught up via InstallSnapshot.
    ///   2. A transient build error (e.g. OOM) — treat as a heartbeat round.
    ///
    /// Only case (1) escalates. We disambiguate via `needsSnapshot` rather than
    /// blindly snapshotting on every null, so a genuine empty heartbeat is never
    /// promoted to a full snapshot (SEC-0144).
    ///
    /// Throttle: while a snapshot is in flight (`pending_snapshot_index != 0`)
    /// we send an empty heartbeat to maintain leadership and only re-ship the
    /// snapshot after `SNAPSHOT_RESEND_TICKS` rounds, so a single laggard does
    /// not cause the whole snapshot to be re-sent every heartbeat (SEC-0144).
    fn escalateOrHeartbeat(self: *RaftNode, rs: *repl.ReplicationState, peer: u64) void {
        const progress = rs.peers.getPtr(peer) orelse {
            self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
            return;
        };

        // Not a compaction gap — the null came from a transient build error.
        // Keep the peer alive with an empty heartbeat; the next round retries
        // the normal AppendEntries path.
        if (!progress.needsSnapshot(self.log.snapshot_index)) {
            self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
            return;
        }

        // A snapshot is already in flight — throttle re-sends.
        if (progress.pending_snapshot_index != 0) {
            progress.pending_snapshot_ticks +|= 1;
            if (progress.pending_snapshot_ticks < progress.snapshotResendTicks(SNAPSHOT_RESEND_TICKS, SNAPSHOT_RESEND_MAX_TICKS)) {
                self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
                return;
            }
            // Retry interval elapsed — fall through and re-send the snapshot.
        }

        if (self.config.stream_snapshots) {
            self.requestSnapshotSend(peer, progress) catch {
                self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
            };
            return;
        }

        self.sendSnapshotTo(peer, progress) catch {
            // No persisted snapshot available (or load/build failure): keep the
            // peer alive with a heartbeat. The compaction gap persists, so the
            // next round re-attempts escalation (SEC-0144 edge case).
            self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
        };
    }

    /// Load the leader's persisted snapshot and ship it to `peer` as a sequence
    /// of InstallSnapshot chunks, recording the in-flight index on `progress`.
    ///
    /// The snapshot is loaded from durable storage (the authoritative copy that
    /// also survived restart) and freed before returning — each chunk message
    /// points into the loaded buffer, so the transport must have copied the
    /// bytes out by the time `sendMsg` returns, which mirrors the
    /// `freeAppendEntriesPayload` contract for AppendEntries (SEC-0144).
    fn sendSnapshotTo(self: *RaftNode, peer: u64, progress: *repl.PeerProgress) !void {
        const loaded = (try self.storage.loadSnapshot()) orelse return error.NoSnapshot;
        defer stg.freeSnapshot(self.allocator, loaded);

        // Resume a transfer of this same snapshot where the follower said it
        // stopped (SONNY-6652); anything else is sent whole.
        const start = if (progress.pending_snapshot_index == loaded.metadata.index) progress.snapshot_resume_offset else 0;
        const msgs = try self.snapshot_state.buildInstallSnapshotMessagesFrom(
            loaded,
            self.current_term,
            self.id,
            start,
        );
        defer self.allocator.free(msgs);

        for (msgs) |m| {
            self.sendMsg(peer, m);
        }

        progress.pending_snapshot_index = loaded.metadata.index;
        progress.pending_snapshot_ticks = 0;
        self.perf_stats.snapshots_sent += 1;
    }

    /// `Config.stream_snapshots`: hand the transfer to the consumer in the
    /// next Ready instead of reading the snapshot here (SONNY-6653). The
    /// snapshot is the one at the log's snapshot point, which the storage
    /// holds (reportSnapshot / commitSnapshot / an installed snapshot persist
    /// it before or as the log moves there).
    fn requestSnapshotSend(self: *RaftNode, peer: u64, progress: *repl.PeerProgress) !void {
        const index = self.log.snapshot_index;
        if (index == 0) return error.NoSnapshot;
        const start = if (progress.pending_snapshot_index == index) progress.snapshot_resume_offset else 0;
        try self.ready_state.addSnapshotSend(.{
            .to = peer,
            .term = self.current_term,
            .leader_id = self.id,
            .index = index,
            .offset = start,
            .chunk_size = self.config.snapshot_chunk_size,
        });
        progress.pending_snapshot_index = index;
        progress.pending_snapshot_ticks = 0;
        self.perf_stats.snapshots_sent += 1;
    }

    fn emptyHeartbeat(term: u64, leader_id: u64, leader_commit: u64) rpc.Message {
        return rpc.Message{ .append_entries = .{
            .term = term,
            .leader_id = leader_id,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{},
            .leader_commit = leader_commit,
        } };
    }

    fn maybeCommit(self: *RaftNode) void {
        if (self.replication_state) |*rs| {
            const cfg = self.membership_state.config();
            _ = rs.maybeCommit(
                &self.log,
                cfg.voters,
                cfg.voters_outgoing,
                self.current_term,
                self.id,
            ) catch return;
        }
    }

    fn sendMsg(self: *RaftNode, to: u64, msg: rpc.Message) void {
        if (self.transport) |t| {
            t.send(to, msg);
        }
    }

    /// Queue a message that announces state not yet durable (a granted vote,
    /// a new term, appended entries, an installed snapshot) in the Ready, for
    /// the consumer to send after `advancePersist()`. Sending it from inside
    /// step() / tick() let a crash before the save lose the vote or the
    /// entries the message acknowledged (Raft §5.2, §5.3; SONNY-6592). Only
    /// messages without borrowed slices are queued. A message that cannot be
    /// queued (OOM) is dropped: the peer retries.
    fn sendAfterPersist(self: *RaftNode, to: u64, msg: rpc.Message) void {
        self.ready_state.addMessage(to, msg) catch {};
    }

    /// Free the entries (slice and data) inside an AppendEntries message
    /// built by `buildAppendEntries` (via `log.slice()`).
    /// Must be called after `sendMsg`: a transport that keeps the message
    /// past `send` must deep-copy the entries, data included (SEC-0140,
    /// SONNY-6592).
    fn freeAppendEntriesPayload(self: *RaftNode, msg: rpc.Message) void {
        switch (msg) {
            .append_entries => |ae| rpc.freeEntries(self.allocator, ae.entries),
            else => {},
        }
    }

    /// Return a snapshot of the current performance counters (PERF-020).
    pub fn stats(self: *const RaftNode) Stats {
        return self.perf_stats;
    }

    fn currentHardState(self: *const RaftNode) st.HardState {
        return .{ .current_term = self.current_term, .voted_for = self.voted_for };
    }
};

// ===========================================================================
// Tests
// ===========================================================================

test "init as follower" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .rng_seed = 42,
    }, mem_store.storage());
    defer node.deinit();

    try std.testing.expectEqual(st.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
    try std.testing.expectEqual(@as(?u64, null), node.leader_id);
    try std.testing.expectEqual(@as(?u64, null), node.voted_for);
    try std.testing.expectEqual(@as(u64, 1), node.id);
}

test "propose on non-leader returns error" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .rng_seed = 42,
    }, mem_store.storage());
    defer node.deinit();

    try std.testing.expectError(error.NotLeader, node.propose("hello"));
}

test "single node becomes leader" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var ipt = trans.InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();
    try ipt.register(1);

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = true,
    }, mem_store.storage());
    defer node.deinit();
    node.setTransport(ipt.transport());

    // Tick until election timeout fires.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }

    try std.testing.expectEqual(st.Role.leader, node.role);
    try std.testing.expectEqual(@as(?u64, 1), node.leader_id);
    // Term should be 1 after a real election (pre-vote doesn't increment, then real does).
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
}

test "leader appends noop on election" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    // Tick until election.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }

    try std.testing.expectEqual(st.Role.leader, node.role);

    // The log should contain a noop entry from the leader.
    const last_idx = node.log.lastIndex();
    try std.testing.expect(last_idx >= 1);

    const last_term = try node.log.term(last_idx);
    try std.testing.expectEqual(node.current_term, last_term);

    // Verify it is a noop.
    const entries = try node.log.slice(last_idx, last_idx + 1);
    defer rpc.freeEntries(std.testing.allocator, entries);
    try std.testing.expectEqual(rpc.EntryKind.noop, entries[0].kind);
}

test "propose on leader succeeds" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    // Become leader.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node.role);

    const before = node.log.lastIndex();
    try node.propose("test-data");
    const after = node.log.lastIndex();

    try std.testing.expectEqual(before + 1, after);

    // Verify the entry has the right data.
    const entries = try node.log.slice(after, after + 1);
    defer rpc.freeEntries(std.testing.allocator, entries);
    try std.testing.expectEqual(rpc.EntryKind.normal, entries[0].kind);
}

test "step with higher term becomes follower" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    // Become leader first.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node.role);

    const leader_term = node.current_term;

    // Receive an AppendEntries with a higher term.
    try node.step(2, rpc.Message{ .append_entries = .{
        .term = leader_term + 5,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });

    try std.testing.expectEqual(st.Role.follower, node.role);
    try std.testing.expectEqual(leader_term + 5, node.current_term);
    try std.testing.expectEqual(@as(?u64, 2), node.leader_id);
}

test "proposeConfigChange rejects oversized entry without corrupting state (SEC-0029)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    // Set max_entry_size very small so that a joint config entry exceeds it.
    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
        .max_entry_size = 10, // Tiny: joint config for {1}+{2,3,4} = 1+4+8+4+3*8 = 41 bytes
    }, mem_store.storage());
    defer node.deinit();

    // Become leader.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node.role);

    // Capture state before the rejected proposal.
    const voters_before = node.membership_state.voters.items.len;
    const is_joint_before = node.membership_state.is_joint;
    const pending_before = node.membership_state.pending_config_change;
    const log_last_before = node.log.lastIndex();

    // This should fail with EntryTooLarge, NOT corrupt membership state.
    const result = node.proposeConfigChange(&.{ 2, 3, 4 });
    try std.testing.expectError(error.EntryTooLarge, result);

    // Verify membership state is unchanged (SEC-0029: no corruption).
    try std.testing.expectEqual(voters_before, node.membership_state.voters.items.len);
    try std.testing.expectEqual(is_joint_before, node.membership_state.is_joint);
    try std.testing.expectEqual(pending_before, node.membership_state.pending_config_change);
    try std.testing.expectEqual(log_last_before, node.log.lastIndex());
}

test "Config.validate rejects duplicate peer IDs (SEC-0033)" {
    const cfg = Config{ .id = 1, .peers = &.{ 2, 3, 2 } };
    try std.testing.expectError(ConfigError.DuplicatePeerId, cfg.validate());
}

test "Config.validate rejects self in peers (SEC-0033)" {
    const cfg = Config{ .id = 1, .peers = &.{ 1, 2, 3 } };
    try std.testing.expectError(ConfigError.SelfInPeers, cfg.validate());
}

test "Config.validate accepts valid unique peers (SEC-0033)" {
    const cfg = Config{ .id = 1, .peers = &.{ 2, 3, 4 } };
    try cfg.validate();
}

test "Config.validate rejects small snapshot_chunk_size (SEC-0100)" {
    const too_small = Config{ .id = 1, .peers = &.{ 2, 3 }, .snapshot_chunk_size = 1 };
    try std.testing.expectError(ConfigError.InvalidSnapshotChunkSize, too_small.validate());

    const at_boundary = Config{ .id = 1, .peers = &.{ 2, 3 }, .snapshot_chunk_size = 1023 };
    try std.testing.expectError(ConfigError.InvalidSnapshotChunkSize, at_boundary.validate());

    const min_valid = Config{ .id = 1, .peers = &.{ 2, 3 }, .snapshot_chunk_size = 1024 };
    try min_valid.validate();
}

test "stats returns correct counters after tick and propose (QA-010)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    // Initial stats should be all zeros.
    var s = node.stats();
    try std.testing.expectEqual(@as(u64, 0), s.ticks);
    try std.testing.expectEqual(@as(u64, 0), s.proposals);
    try std.testing.expectEqual(@as(u64, 0), s.elections_started);
    try std.testing.expectEqual(@as(u64, 0), s.became_leader);

    // Tick until leader (single-node cluster).
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node.role);

    s = node.stats();
    try std.testing.expect(s.ticks > 0);
    try std.testing.expect(s.elections_started > 0);
    try std.testing.expectEqual(@as(u64, 1), s.became_leader);

    // Advance the noop from leader election before proposing.
    _ = try node.ready();
    try node.advance();

    // Propose two entries and verify the proposals counter.
    try node.propose("entry-1");
    try node.propose("entry-2");
    s = node.stats();
    try std.testing.expectEqual(@as(u64, 2), s.proposals);

    // The entries commit once advance() has persisted them (SONNY-6611),
    // and are applied by the Ready after that.
    try node.tick(null);
    _ = try node.ready();
    try node.advance();
    _ = try node.ready();
    try node.advance();
    s = node.stats();
    // The 2 proposed entries should be applied (noop was applied earlier).
    try std.testing.expect(s.entries_applied >= 2);
}

test "advancePersist then advanceApply produces same result as advance (QA-010)" {
    // Test with split advance: create two identical single-node leaders,
    // advance one with advance() and the other with advancePersist()+advanceApply().
    // Verify both reach the same state.

    // --- Node A: uses advance() ---
    var store_a = stg.InMemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();

    var node_a = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, store_a.storage());
    defer node_a.deinit();

    // --- Node B: uses advancePersist + advanceApply ---
    var store_b = stg.InMemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var node_b = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, store_b.storage());
    defer node_b.deinit();

    // Tick both until leader.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node_a.tick(null);
        try node_b.tick(null);
        if (node_a.role == .leader and node_b.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node_a.role);
    try std.testing.expectEqual(st.Role.leader, node_b.role);

    // ready + advance on A (noop from election).
    _ = try node_a.ready();
    try node_a.advance();

    // ready + split advance on B (noop from election).
    _ = try node_b.ready();
    try node_b.advancePersist();
    try node_b.advanceApply();

    // Both nodes should have identical state after advancing the noop.
    try std.testing.expectEqual(node_a.current_term, node_b.current_term);
    try std.testing.expectEqual(node_a.log.lastIndex(), node_b.log.lastIndex());
    try std.testing.expectEqual(node_a.log.committed, node_b.log.committed);
    try std.testing.expectEqual(node_a.log.applied, node_b.log.applied);
    try std.testing.expectEqual(node_a.stats().entries_applied, node_b.stats().entries_applied);

    // Propose entries on both.
    try node_a.propose("data-1");
    try node_b.propose("data-1");

    // Tick to run maybeCommit() so the commit index advances.
    try node_a.tick(null);
    try node_b.tick(null);

    // Advance A with advance(), B with split.
    _ = try node_a.ready();
    try node_a.advance();

    _ = try node_b.ready();
    try node_b.advancePersist();
    try node_b.advanceApply();

    // Verify state still matches.
    try std.testing.expectEqual(node_a.log.lastIndex(), node_b.log.lastIndex());
    try std.testing.expectEqual(node_a.log.committed, node_b.log.committed);
    try std.testing.expectEqual(node_a.log.applied, node_b.log.applied);
    try std.testing.expectEqual(node_a.stats().proposals, node_b.stats().proposals);
    try std.testing.expectEqual(node_a.stats().entries_applied, node_b.stats().entries_applied);
}

// ---------------------------------------------------------------------------
// Auto-driven InstallSnapshot escalation (SEC-0144)
// ---------------------------------------------------------------------------

/// Capturing transport for snapshot-escalation tests. Records only the tag and
/// scalar fields of each message (never the slices), so it is safe to read
/// after `sendSnapshotTo` frees the loaded snapshot.
const CapturingTransport = struct {
    last_to: u64 = 0,
    install_snapshot_count: u32 = 0,
    empty_ae_count: u32 = 0,
    nonempty_ae_count: u32 = 0,
    last_snapshot_index: u64 = 0,

    const vtable_inst = trans.Transport.VTable{ .send = &sendImpl };

    fn transport(self: *CapturingTransport) trans.Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_inst };
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: rpc.Message) void {
        const self: *CapturingTransport = @ptrCast(@alignCast(ptr));
        self.last_to = to;
        switch (msg) {
            .install_snapshot => |is| {
                self.install_snapshot_count += 1;
                self.last_snapshot_index = is.last_included_index;
            },
            .append_entries => |ae| {
                if (ae.entries.len == 0) self.empty_ae_count += 1 else self.nonempty_ae_count += 1;
            },
            else => {},
        }
    }
};

/// Drive a single-voter leader (id 1, no peers) to leader, commit a few
/// entries, then snapshot+compact so any peer below the snapshot boundary can
/// only be caught up via InstallSnapshot. A single voter lets the leader commit
/// on its own; `peer_id` is injected into replication_state afterwards so the
/// escalation path has a target without needing a second live node.
fn makeCompactedLeader(node: *RaftNode, peer_id: u64) !void {
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try node.tick(null);
        if (node.role == .leader) break;
    }
    try std.testing.expectEqual(st.Role.leader, node.role);

    // Apply the election noop, then propose + locally commit a few entries.
    _ = try node.ready();
    try node.advance();

    var j: u32 = 0;
    while (j < 4) : (j += 1) {
        try node.propose("payload");
    }
    // Single-voter quorum is just the leader, so committing is immediate on tick.
    try node.tick(null);
    _ = try node.ready();
    try node.advance();

    // Snapshot at the applied index and compact the log past it.
    try node.reportSnapshot("snap-payload");
    try node.advance();

    // Inject the laggard peer into replication_state as the snapshot target.
    var rs = &(node.replication_state.?);
    try rs.peers.put(peer_id, .{ .next_index = node.log.lastIndex() + 1, .match_index = 0 });
}

test "escalateOrHeartbeat sends InstallSnapshot for compacted peer and sets pending (SEC-0144)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    var capture = CapturingTransport{};
    node.setTransport(capture.transport());

    try makeCompactedLeader(&node, 2);
    try std.testing.expect(node.log.snapshot_index > 0);

    // Force the peer's next_index into the compacted region.
    var rs = &(node.replication_state.?);
    const progress = rs.peers.getPtr(2).?;
    progress.next_index = node.log.snapshot_index; // <= snapshot_index => needs snapshot.
    try std.testing.expect(progress.needsSnapshot(node.log.snapshot_index));

    const sent_before = node.stats().snapshots_sent;
    node.escalateOrHeartbeat(rs, 2);

    // An InstallSnapshot must have been sent, pending set, stat incremented.
    try std.testing.expectEqual(@as(u32, 1), capture.install_snapshot_count);
    try std.testing.expectEqual(@as(u32, 0), capture.empty_ae_count);
    try std.testing.expectEqual(node.log.snapshot_index, progress.pending_snapshot_index);
    try std.testing.expectEqual(sent_before + 1, node.stats().snapshots_sent);
}

test "escalateOrHeartbeat throttles re-send while snapshot pending (SEC-0144)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    var capture = CapturingTransport{};
    node.setTransport(capture.transport());

    try makeCompactedLeader(&node, 2);

    var rs = &(node.replication_state.?);
    const progress = rs.peers.getPtr(2).?;
    progress.next_index = node.log.snapshot_index;

    // First call ships the snapshot.
    node.escalateOrHeartbeat(rs, 2);
    try std.testing.expectEqual(@as(u32, 1), capture.install_snapshot_count);

    // Subsequent calls within the retry window send empty heartbeats, NOT a
    // re-shipped snapshot, until SNAPSHOT_RESEND_TICKS rounds elapse.
    var r: u32 = 0;
    while (r < SNAPSHOT_RESEND_TICKS - 1) : (r += 1) {
        node.escalateOrHeartbeat(rs, 2);
    }
    try std.testing.expectEqual(@as(u32, 1), capture.install_snapshot_count);
    try std.testing.expectEqual(SNAPSHOT_RESEND_TICKS - 1, capture.empty_ae_count);

    // The next call crosses the retry interval and re-ships the snapshot.
    node.escalateOrHeartbeat(rs, 2);
    try std.testing.expectEqual(@as(u32, 2), capture.install_snapshot_count);
}

test "handleInstallSnapshotResponse clears pending on success and failure (SEC-0144)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    var capture = CapturingTransport{};
    node.setTransport(capture.transport());

    try makeCompactedLeader(&node, 2);

    var rs = &(node.replication_state.?);
    const progress = rs.peers.getPtr(2).?;
    progress.next_index = node.log.snapshot_index;
    node.escalateOrHeartbeat(rs, 2);
    try std.testing.expect(progress.pending_snapshot_index != 0);

    // A successful ack advances next_index past the snapshot and clears pending.
    try node.handleInstallSnapshotResponse(2, .{ .term = node.current_term, .success = true });
    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot_index);
    try std.testing.expectEqual(@as(u32, 0), progress.pending_snapshot_ticks);
    try std.testing.expect(progress.next_index > node.log.snapshot_index);

    // Re-arm pending, then a failure ack keeps it in flight and backs off
    // (SONNY-6636): the follower refused this snapshot, and sending it again
    // at once would be refused the same way.
    progress.next_index = node.log.snapshot_index;
    node.escalateOrHeartbeat(rs, 2);
    try std.testing.expect(progress.pending_snapshot_index != 0);
    try node.handleInstallSnapshotResponse(2, .{ .term = node.current_term, .success = false });
    try std.testing.expect(progress.pending_snapshot_index != 0);
    try std.testing.expectEqual(@as(u32, 0), progress.pending_snapshot_ticks);
    try std.testing.expectEqual(@as(u32, 1), progress.snapshot_refusals);
}

test "a refused InstallSnapshot is re-sent with exponential backoff, reset by an install (SONNY-6636)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{},
        .election_tick = 5,
        .rng_seed = 42,
        .pre_vote = false,
    }, mem_store.storage());
    defer node.deinit();

    var capture = CapturingTransport{};
    node.setTransport(capture.transport());
    try makeCompactedLeader(&node, 2);

    var rs = &(node.replication_state.?);
    const progress = rs.peers.getPtr(2).?;
    progress.next_index = node.log.snapshot_index;

    // The follower refuses every snapshot (a digest it cannot match, a store
    // that cannot take it). Before, the leader re-sent it every round.
    var answered: u32 = 0;
    var round: u32 = 0;
    while (round < 200) : (round += 1) {
        node.escalateOrHeartbeat(rs, 2);
        while (answered < capture.install_snapshot_count) : (answered += 1) {
            try node.handleInstallSnapshotResponse(2, .{ .term = node.current_term, .success = false });
        }
    }
    // Sent at rounds 0, 16, 48, 112 (waits 16, 32, 64, then 128).
    try std.testing.expectEqual(@as(u32, 4), capture.install_snapshot_count);
    try std.testing.expectEqual(@as(u32, 4), progress.snapshot_refusals);

    // The wait is capped.
    progress.snapshot_refusals = 30;
    try std.testing.expectEqual(SNAPSHOT_RESEND_MAX_TICKS, progress.snapshotResendTicks(SNAPSHOT_RESEND_TICKS, SNAPSHOT_RESEND_MAX_TICKS));

    // An install resets it.
    try node.handleInstallSnapshotResponse(2, .{ .term = node.current_term, .success = true });
    try std.testing.expectEqual(@as(u32, 0), progress.snapshot_refusals);
    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot_index);
}

test "a node outside its own voter set does not start elections" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();
    // Restored from a snapshot whose config no longer includes node 1
    // (it was removed by a membership change).
    try mem_store.storage().saveSnapshot(.{
        .metadata = .{ .index = 5, .term = 2, .config = .{
            .voters = &.{ 2, 3 },
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "s",
    });

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .pre_vote = false,
        .rng_seed = 3,
    }, mem_store.storage());
    defer node.deinit();

    var i: u32 = 0;
    while (i < 100) : (i += 1) try node.tick(null);
    try std.testing.expectEqual(st.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 0), node.stats().elections_started);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
}
