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

/// Maximum number of peers that can be served from a stack buffer
/// without heap allocation (PERF-004, QA-006). Covers clusters up to
/// 16 voters which is well beyond practical Raft deployments (3-7).
pub const MAX_STACK_PEERS: usize = 16;

pub const ConfigError = error{
    InvalidId,
    InvalidElectionTick,
    InvalidHeartbeatTick,
    HeartbeatNotLessThanElection,
    InvalidSnapshotChunkSize,
    DuplicatePeerId,
    SelfInPeers,
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
    rng_seed: ?u64 = null,

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
    /// Number of snapshots installed (follower receiving from leader).
    snapshots_installed: u64 = 0,
    /// Number of ticks processed.
    ticks: u64 = 0,
    /// Number of messages stepped (RPC messages processed via step()).
    messages_stepped: u64 = 0,
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
                var all_voters = std.ArrayList(u64){};
                defer all_voters.deinit(allocator);
                try all_voters.append(allocator, config.id);
                try all_voters.appendSlice(allocator, config.peers);
                membership_inst = try mem.MembershipState.init(allocator, all_voters.items);
            }
        } else {
            // No snapshot — use static config.peers.
            var all_voters = std.ArrayList(u64){};
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
                }
            }

            // Check if leader should step down.
            if (self.membership_state.shouldLeaderStepDown(self.id)) {
                self.becomeFollower(self.current_term, null);
                return;
            }

            // Try to advance commit index.
            self.maybeCommit();
        } else {
            self.election_state.tickElection();
            if (self.election_state.electionTimeoutElapsed()) {
                try self.startElection(self.config.pre_vote);
            }
        }
    }

    pub fn ready(self: *RaftNode) !rd.Ready {
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
                // Free slice on error so addCommittedEntries failure doesn't leak (SEC-0117).
                errdefer if (entries.len > 0) self.allocator.free(entries);

                try self.ready_state.addCommittedEntries(entries);

                // Free the slice allocated by log.slice() — entries have been copied into ready_state.
                if (entries.len > 0) {
                    self.allocator.free(entries);
                }
            }
        }

        // Check shouldSnapshot.
        if (self.snapshot_state.shouldSnapshot(self.log.lastIndex())) {
            self.ready_state.setShouldSnapshot(true);
        }

        return try self.ready_state.ready(self.currentHardState());
    }

    /// Convenience method that calls advancePersist() then advanceApply().
    /// Use the split methods directly for pipelined persist+send (PERF-006).
    pub fn advance(self: *RaftNode) !void {
        try self.advancePersist();
        try self.advanceApply();
    }

    /// Phase 1: persist hard state and unstable entries to storage.
    /// After this returns, the consumer may send Ready.messages over the
    /// network in parallel with disk I/O completion (PERF-006).
    pub fn advancePersist(self: *RaftNode) !void {
        const hs = self.currentHardState();

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
        }
    }

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
        var to_remove = std.ArrayList(u64){};
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
            self.sendMsg(peer, msg);
        }
    }

    fn handleRequestVote(self: *RaftNode, from: u64, msg: rpc.RequestVote) !void {
        _ = from;

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
            self.sendMsg(msg.candidate_id, rpc.Message{ .request_vote_response = .{
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
                self.sendMsg(msg.candidate_id, rpc.Message{ .request_vote_response = .{
                    .term = self.current_term,
                    .vote_granted = false,
                    .pre_vote = true,
                } });
                return;
            }
            if (self.leader_id != null and msg.term <= self.current_term) {
                self.sendMsg(msg.candidate_id, rpc.Message{ .request_vote_response = .{
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
        self.sendMsg(msg.candidate_id, response);
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
        }
    }

    fn handleAppendEntries(self: *RaftNode, from: u64, msg: rpc.AppendEntries) !void {
        _ = from;

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
    }

    /// Send a rejection AppendEntries response to the given leader (QA-002).
    /// Used for stale-term, malformed, or invalid AppendEntries messages.
    fn rejectAppendEntries(self: *RaftNode, leader_id: u64) void {
        self.sendMsg(leader_id, rpc.Message{ .append_entries_response = .{
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
            self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                .term = self.current_term,
                .success = true,
                .match_index = match_index,
                .conflict_index = 0,
                .conflict_term = 0,
            } });
        } else {
            const conflict = self.log.findConflict(msg.prev_log_index);
            self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                .term = self.current_term,
                .success = false,
                .match_index = 0,
                .conflict_index = conflict.index,
                .conflict_term = conflict.term_val,
            } });
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
            if (progress.maybeDecrTo(msg.match_index, msg.conflict_index, msg.conflict_term)) {
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
                }
            }
        }
    }

    fn handleInstallSnapshot(self: *RaftNode, from: u64, msg: rpc.InstallSnapshot) !void {
        _ = from;

        // Reject messages from node ID 0 — invalid sender (SEC-0135).
        if (msg.leader_id == 0) return;

        // Reject requests from non-member nodes (SEC-0134).
        if (!mem.isVoter(self.membership_state.config(), msg.leader_id)) {
            std.log.warn("raft: InstallSnapshot from non-member {d}, ignoring", .{msg.leader_id});
            return;
        }

        if (msg.term < self.current_term) {
            self.sendMsg(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = false,
            } });
            return;
        }

        self.becomeFollower(msg.term, msg.leader_id);
        self.election_state.resetElectionElapsed();

        const result = self.snapshot_state.receiveChunk(msg) catch |err| {
            std.log.warn("InstallSnapshot from leader {d} failed: {s}", .{
                msg.leader_id, @errorName(err),
            });
            self.sendMsg(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = false,
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
            // Transfer ownership to ready_state — errdefer will not fire
            // on normal return since setSnapshot is infallible.
            self.ready_state.setSnapshot(snapshot_val);
            self.perf_stats.snapshots_installed += 1;
            self.sendMsg(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = true,
            } });
        }
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
            // Use log.snapshot_index (persisted) rather than
            // snapshot_state.snapshot_index which may have advanced since
            // the snapshot was sent (SEC-0113).
            _ = progress.maybeUpdate(self.log.snapshot_index);
            self.maybeCommit();
        }
    }

    /// Build and send AppendEntries heartbeats to all peers.
    /// When `pool` is non-null, AppendEntries payloads for each peer
    /// are constructed in parallel using thread-pool submitBatch/WaitGroup.
    fn sendHeartbeats(self: *RaftNode, pool: ?*tp.ThreadPool) !void {
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
            _ = p.submitBatch(batch, .{ .wait_group = &wg }) catch {
                // Queue full: fall back to sequential build.
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
            };
            wg.wait();

            // Send results.
            for (peers, 0..) |peer, i| {
                if (results[i]) |m| {
                    self.sendMsg(peer, m);
                    // Free the entries slice allocated by buildAppendEntries (SEC-0140).
                    self.freeAppendEntriesPayload(m);
                } else {
                    self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
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
                    self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
                }
            }
        }
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

    /// Free the heap-allocated entries slice inside an AppendEntries message
    /// that was built by `buildAppendEntries` (via `log.slice()`).
    /// Must be called after `sendMsg` since transport copies by value (SEC-0140).
    fn freeAppendEntriesPayload(self: *RaftNode, msg: rpc.Message) void {
        switch (msg) {
            .append_entries => |ae| {
                if (ae.entries.len > 0) {
                    self.allocator.free(ae.entries);
                }
            },
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
    defer std.testing.allocator.free(entries);
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
    defer std.testing.allocator.free(entries);
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

    // Tick to run maybeCommit() so the commit index advances.
    try node.tick(null);

    // After ready+advance, entries_applied should increment.
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
