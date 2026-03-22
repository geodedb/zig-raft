// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const spice = @import("spice");
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
        if (self.snapshot_chunk_size == 0) return ConfigError.InvalidSnapshotChunkSize;

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
    /// `spice.Task` to `tick()` or `step()`, parallel worker threads will
    /// allocate from this allocator concurrently (e.g. in `parallelBuildAE`,
    /// `parallelSendVotes`, `parallelSendHeartbeats`). In that case, the
    /// allocator **must** be thread-safe. For example, use
    /// `std.heap.GeneralPurposeAllocator(.{ .thread_safe = true })` or
    /// `std.heap.page_allocator`. If `task` is always null (sequential mode),
    /// any allocator is safe.
    allocator: std.mem.Allocator,

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Initialize a new RaftNode.
    ///
    /// The `allocator` must be thread-safe if spice parallelism will be used
    /// (i.e. non-null `task` parameters passed to `tick()`/`step()`). See
    /// the `allocator` field doc comment for details (SEC-0067).
    pub fn init(allocator: std.mem.Allocator, config: Config, storage_impl: stg.Storage) !RaftNode {
        try config.validate();

        const log_inst = try raft_log.RaftLog.init(allocator, storage_impl);

        // Load snapshot to restore membership config (SEC-0073).
        // RaftLog.init already loaded and freed its own copy for index/term.
        // This second load retrieves the config for MembershipState restoration.
        const loaded_snap = try storage_impl.loadSnapshot();

        // Build initial voter set: use snapshot config if available,
        // otherwise fall back to static config.peers (SEC-0073).
        var membership_inst: mem.MembershipState = undefined;
        if (loaded_snap) |snap_val| {
            defer stg.freeSnapshot(allocator, snap_val);
            if (snap_val.metadata.config.voters.len > 0) {
                // Restore membership from snapshot config (SEC-0073).
                membership_inst = try mem.MembershipState.init(allocator, snap_val.metadata.config.voters);
                if (snap_val.metadata.config.is_joint and snap_val.metadata.config.voters_outgoing.len > 0) {
                    try membership_inst.voters_outgoing.appendSlice(allocator, snap_val.metadata.config.voters_outgoing);
                    membership_inst.is_joint = true;
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
            .pending_config_index = 0,
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
            .ready_state = rd.ReadyState.init(allocator, st.HardState.initial),
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
        const result = try self.allocator.alloc(u64, count);
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

        try self.log.append(&.{entry});
        self.pending_config_index = entry.index;
    }

    /// Process an inbound RPC message from another node.
    /// When `task` is non-null, spice parallelism may be used for
    /// read-only operations (e.g. building AppendEntries responses).
    ///
    /// NOTE: This library validates that `from` matches the sender ID
    /// embedded in messages (e.g. candidate_id, leader_id) where applicable.
    /// However, the `from` parameter itself is supplied by the transport layer.
    /// Transport implementations MUST authenticate peers (e.g. via mTLS or
    /// HMAC-signed messages) to prevent impersonation attacks.
    pub fn step(self: *RaftNode, from: u64, msg: rpc.Message, task: ?*spice.Task) !void {
        _ = task; // Reserved for future parallel response construction.

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
    /// When `task` is non-null, spice parallelism is used for building
    /// AppendEntries payloads and broadcasting votes in parallel.
    pub fn tick(self: *RaftNode, task: ?*spice.Task) !void {
        if (self.role == .leader) {
            self.election_state.tickHeartbeat();
            if (self.election_state.heartbeatTimeoutElapsed()) {
                self.election_state.resetHeartbeatElapsed();
                try self.sendHeartbeats(task);
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
                try self.startElection(self.config.pre_vote, task);
            }
        }
    }

    pub fn ready(self: *RaftNode) !rd.Ready {
        // Collect committed entries.
        if (self.log.committed > self.log.applied) {
            // Checked overflow: applied + 1, committed + 1 (SEC-0071).
            const slice_lo = std.math.add(u64, self.log.applied, 1) catch return error.Overflow;
            const slice_hi = std.math.add(u64, self.log.committed, 1) catch return error.Overflow;
            const entries = try self.log.slice(
                slice_lo,
                slice_hi,
            );

            try self.ready_state.addCommittedEntries(entries);

            // Free the slice allocated by log.slice() — entries have been copied into ready_state.
            if (entries.len > 0) {
                self.allocator.free(entries);
            }
        }

        // Check shouldSnapshot.
        if (self.snapshot_state.shouldSnapshot(self.log.lastIndex())) {
            self.ready_state.setShouldSnapshot(true);
        }

        return try self.ready_state.ready(self.currentHardState());
    }

    pub fn advance(self: *RaftNode) !void {
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

        // Apply committed config entries to MembershipState (SEC-0073).
        // This ensures followers (and leaders after restart) update their
        // voter sets when config change entries are committed.
        // Propagate errors instead of swallowing them (SEC-0079).
        if (self.log.committed > self.log.applied) {
            const apply_lo = std.math.add(u64, self.log.applied, 1) catch return error.Overflow;
            const apply_hi = std.math.add(u64, self.log.committed, 1) catch return error.Overflow;
            const entries_to_apply = try self.log.slice(apply_lo, apply_hi);
            defer if (entries_to_apply.len > 0) self.allocator.free(entries_to_apply);
            var had_config = false;
            for (entries_to_apply) |entry| {
                if (entry.kind == .config and entry.data.len > 0) {
                    try self.membership_state.applyConfig(entry);
                    had_config = true;
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
            try self.log.appliedTo(self.log.committed);
        }

        // Advance ready state.
        self.ready_state.advance(hs);

        // Stabilize log entries.
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
        self.role = .leader;
        self.leader_id = self.id;
        self.is_pre_vote_election = false;

        // Initialize replication_state with dynamic voters (SEC-0044).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 0) self.allocator.free(peers);
        self.replication_state = try repl.ReplicationState.init(
            self.allocator,
            peers,
            self.log.lastIndex(),
        );

        self.election_state.resetHeartbeatElapsed();

        // Append no-op entry (critical for correctness, section 2.7).
        // Checked overflow: lastIndex + 1 (SEC-0071).
        const noop_index = std.math.add(u64, self.log.lastIndex(), 1) catch return error.Overflow;
        const noop = rpc.Entry{
            .index = noop_index,
            .term = self.current_term,
            .kind = .noop,
            .data = &.{},
        };
        try self.log.append(&.{noop});
    }

    /// Synchronize replication_state.peers with the current voter set.
    /// Adds new voters, removes departed voters, preserves existing progress (SEC-0075).
    fn syncReplicationPeers(self: *RaftNode) !void {
        var rs = &(self.replication_state orelse return);
        const peers = try self.dynamicPeers();
        defer if (peers.len > 0) self.allocator.free(peers);

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

    fn startElection(self: *RaftNode, pre_vote: bool, task: ?*spice.Task) !void {
        if (pre_vote) {
            self.is_pre_vote_election = true;
            // Don't increment term or set voted_for for pre-vote.
        } else {
            self.current_term = std.math.add(u64, self.current_term, 1) catch return error.Overflow;
            self.voted_for = self.id;
            self.is_pre_vote_election = false;
        }

        self.role = .candidate;
        self.election_state.resetElectionElapsed();
        self.election_state.resetElectionTimeout();

        // Reset votes, record own vote.
        self.election_state.resetVotes();
        try self.election_state.recordVote(self.id, true);

        // Use dynamic voters for election (SEC-0044).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 0) self.allocator.free(peers);

        // Single-node cluster: become leader immediately.
        if (peers.len == 0) {
            if (pre_vote) {
                // Pre-vote succeeded (self-only), start real election.
                try self.startElection(false, task);
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

        // If msg.term > current_term and not a pre-vote, step down.
        if (msg.term > self.current_term and !msg.pre_vote) {
            self.becomeFollower(msg.term, null);
        }

        // Leaders must NOT grant pre-votes to prevent disruptive re-elections
        // from partitioned nodes (Raft §9.6, SEC-0061).
        if (msg.pre_vote and self.role == .leader) {
            const response = rpc.Message{ .request_vote_response = .{
                .term = self.current_term,
                .vote_granted = false,
                .pre_vote = true,
            } };
            self.sendMsg(msg.candidate_id, response);
            return;
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

        // Validate sender is a known voter before recording (SEC-0083).
        const cfg = self.membership_state.config();
        var is_voter = false;
        for (cfg.voters) |v| {
            if (v == from) {
                is_voter = true;
                break;
            }
        }
        if (!is_voter) {
            for (cfg.voters_outgoing) |v| {
                if (v == from) {
                    is_voter = true;
                    break;
                }
            }
        }
        if (!is_voter) {
            std.log.warn("raft: RequestVoteResponse from non-voter {d}, ignoring", .{from});
            return;
        }

        try self.election_state.recordVote(from, msg.vote_granted);

        // Check quorum (reuse cfg from voter validation above).
        if (self.election_state.hasJointQuorum(cfg.voters, cfg.voters_outgoing)) {
            if (self.is_pre_vote_election) {
                try self.startElection(false, null);
            } else {
                try self.becomeLeader();
            }
        }
    }

    fn handleAppendEntries(self: *RaftNode, from: u64, msg: rpc.AppendEntries) !void {
        _ = from;

        if (msg.term < self.current_term) {
            // Reject: stale term.
            self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                .term = self.current_term,
                .success = false,
                .match_index = 0,
                .conflict_index = 0,
                .conflict_term = 0,
            } });
            return;
        }

        // msg.term >= current_term: accept leader.
        self.becomeFollower(msg.term, msg.leader_id);

        self.election_state.resetElectionElapsed();

        // Reject if entry count exceeds max_entries_per_msg or any entry
        // exceeds max_entry_size (SEC-0082).
        if (msg.entries.len > self.config.max_entries_per_msg) {
            self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                .term = self.current_term,
                .success = false,
                .match_index = 0,
                .conflict_index = 0,
                .conflict_term = 0,
            } });
            return;
        }
        if (self.config.max_entry_size > 0) {
            var total_bytes: u64 = 0;
            for (msg.entries) |entry| {
                if (entry.data.len > self.config.max_entry_size) {
                    self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                        .term = self.current_term,
                        .success = false,
                        .match_index = 0,
                        .conflict_index = 0,
                        .conflict_term = 0,
                    } });
                    return;
                }
                total_bytes = std.math.add(u64, total_bytes, entry.data.len) catch {
                    self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                        .term = self.current_term,
                        .success = false,
                        .match_index = 0,
                        .conflict_index = 0,
                        .conflict_term = 0,
                    } });
                    return;
                };
            }
            if (total_bytes > self.config.max_bytes_per_msg) {
                self.sendMsg(msg.leader_id, rpc.Message{ .append_entries_response = .{
                    .term = self.current_term,
                    .success = false,
                    .match_index = 0,
                    .conflict_index = 0,
                    .conflict_term = 0,
                } });
                return;
            }
        }

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
                }
            }
        }
    }

    fn handleInstallSnapshot(self: *RaftNode, from: u64, msg: rpc.InstallSnapshot) !void {
        _ = from;

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
            // Restore log state from the snapshot so committed/applied/
            // snapshot_index track the snapshot point (SEC-0090).
            try self.log.restore(snapshot_val);

            // Apply snapshot config to MembershipState so followers have
            // the correct voter set after receiving a snapshot (SEC-0073).
            if (snapshot_val.metadata.config.voters.len > 0) {
                var new_voters = std.ArrayList(u64){};
                try new_voters.appendSlice(self.allocator, snapshot_val.metadata.config.voters);
                errdefer new_voters.deinit(self.allocator);
                var new_outgoing = std.ArrayList(u64){};
                if (snapshot_val.metadata.config.is_joint and snapshot_val.metadata.config.voters_outgoing.len > 0) {
                    try new_outgoing.appendSlice(self.allocator, snapshot_val.metadata.config.voters_outgoing);
                }
                self.membership_state.voters.deinit(self.allocator);
                self.membership_state.voters = new_voters;
                self.membership_state.voters_outgoing.deinit(self.allocator);
                self.membership_state.voters_outgoing = new_outgoing;
                self.membership_state.is_joint = snapshot_val.metadata.config.is_joint;
            }
            self.ready_state.setSnapshot(snapshot_val);
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

        // Update peer progress on successful snapshot install (SEC-0074).
        if (msg.success) {
            // The follower has the snapshot, so its match_index is at least
            // the snapshot index. Update progress so the leader can resume
            // normal replication from the snapshot point.
            _ = progress.maybeUpdate(self.snapshot_state.snapshot_index);
            self.maybeCommit();
        }
    }

    /// Build and send AppendEntries heartbeats to all peers.
    /// When `task` is non-null, AppendEntries payloads for each peer
    /// are constructed in parallel using spice fork/join.
    fn sendHeartbeats(self: *RaftNode, task: ?*spice.Task) !void {
        var rs = &(self.replication_state orelse return);

        // Use dynamic voters instead of static config.peers (SEC-0044).
        const peers = try self.dynamicPeers();
        defer if (peers.len > 0) self.allocator.free(peers);

        if (task) |t| {
            // Parallel path: fork one Future per peer, then join and send.
            // Results are written into a shared array because rpc.Message
            // exceeds spice's 32-byte Future return slot.
            const futures = try self.allocator.alloc(BuildAEFuture, peers.len);
            defer self.allocator.free(futures);
            const results = try self.allocator.alloc(?rpc.Message, peers.len);
            defer self.allocator.free(results);

            for (peers, 0..) |peer, i| {
                results[i] = null;
                futures[i] = BuildAEFuture.init();
                futures[i].fork(t, parallelBuildAE, .{
                    .rs = rs,
                    .peer = peer,
                    .log = &self.log,
                    .current_term = self.current_term,
                    .leader_id = self.id,
                    .committed = self.log.committed,
                    .max_entries_per_msg = self.config.max_entries_per_msg,
                    .max_bytes_per_msg = self.config.max_bytes_per_msg,
                    .out = &results[i],
                });
            }

            for (peers, 0..) |peer, i| {
                // join returns ?void — null means the future wasn't executed
                // (was popped before a worker got to it).
                const did_run = futures[i].join(t);
                if (did_run == null) {
                    // Future didn't execute; build sequentially.
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

                if (results[i]) |m| {
                    self.sendMsg(peer, m);
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
                } else {
                    self.sendMsg(peer, emptyHeartbeat(self.current_term, self.id, self.log.committed));
                }
            }
        }
    }

    /// Input for parallel AppendEntries construction.
    /// Results are written to `out` rather than returned through the Future
    /// (rpc.Message is too large for spice's 32-byte return slot).
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

    const BuildAEFuture = spice.Future(BuildAEInput, void);

    fn parallelBuildAE(_: *spice.Task, input: BuildAEInput) void {
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
    } }, null);

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
