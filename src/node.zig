// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
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

pub const Config = struct {
    id: u64,
    peers: []const u64,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    max_entries_per_msg: u32 = 64,
    max_bytes_per_msg: u64 = 1024 * 1024,
    snapshot_chunk_size: usize = 64 * 1024,
    snapshot_threshold: u32 = 1000,
    pre_vote: bool = true,
    rng_seed: ?u64 = null,
};

pub const NotLeader = struct {
    leader_id: ?u64,
};

pub const RaftNode = struct {
    id: u64,
    role: st.Role,
    leader_id: ?u64,
    current_term: u64,
    voted_for: ?u64,
    is_pre_vote_election: bool,
    pending_config_index: u64,  // index of the joint config entry, 0 = none
    log: raft_log.RaftLog,
    election_state: elect.ElectionState,
    replication_state: ?repl.ReplicationState,
    membership_state: mem.MembershipState,
    snapshot_state: snap.SnapshotState,
    ready_state: rd.ReadyState,
    config: Config,
    transport: ?trans.Transport,
    storage: stg.Storage,
    allocator: std.mem.Allocator,

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    pub fn init(allocator: std.mem.Allocator, config: Config, storage_impl: stg.Storage) !RaftNode {
        var all_voters = std.ArrayList(u64){};
        defer all_voters.deinit(allocator);
        try all_voters.append(allocator, config.id);
        try all_voters.appendSlice(allocator, config.peers);

        var log_inst = try raft_log.RaftLog.init(allocator, storage_impl);

        // Load hard state from storage if available.
        const initial_term: u64 = 0;
        const initial_voted_for: ?u64 = null;
        const loaded_snap = try storage_impl.loadSnapshot();
        _ = loaded_snap;
        // Hard state is not directly loadable from the Storage interface,
        // so we start with term 0. The consumer is expected to restore state
        // via step() or by providing it through storage.
        _ = &log_inst;

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
            .membership_state = try mem.MembershipState.init(allocator, all_voters.items),
            .snapshot_state = snap.SnapshotState.init(
                allocator,
                config.snapshot_threshold,
                config.snapshot_chunk_size,
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

    pub fn propose(self: *RaftNode, data: []const u8) error{NotLeader}!void {
        if (self.role != .leader) return error.NotLeader;

        const entry = rpc.Entry{
            .index = self.log.lastIndex() + 1,
            .term = self.current_term,
            .kind = .normal,
            .data = data,
        };
        // log.append internally duplicates data, no need to pre-copy.
        self.log.append(&.{entry}) catch {};
    }

    pub fn proposeConfigChange(self: *RaftNode, new_peers: []const u64) !void {
        if (self.role != .leader) return error.NotLeader;

        const entry = try self.membership_state.proposeChange(
            new_peers,
            self.current_term,
            self.log.lastIndex() + 1,
        );
        try self.log.append(&.{entry});
        self.pending_config_index = entry.index;
    }

    pub fn step(self: *RaftNode, from: u64, msg: rpc.Message) !void {
        switch (msg) {
            .request_vote => |rv| try self.handleRequestVote(from, rv),
            .request_vote_response => |rvr| try self.handleRequestVoteResponse(from, rvr),
            .append_entries => |ae| try self.handleAppendEntries(from, ae),
            .append_entries_response => |aer| try self.handleAppendEntriesResponse(from, aer),
            .install_snapshot => |is| try self.handleInstallSnapshot(from, is),
            .install_snapshot_response => |isr| try self.handleInstallSnapshotResponse(from, isr),
        }
    }

    pub fn tick(self: *RaftNode) !void {
        if (self.role == .leader) {
            self.election_state.tickHeartbeat();
            if (self.election_state.heartbeatTimeoutElapsed()) {
                self.election_state.resetHeartbeatElapsed();
                try self.sendHeartbeats();
            }

            // Check if joint config committed and needs phase-2.
            if (self.membership_state.is_joint and self.pending_config_index > 0 and self.log.committed >= self.pending_config_index) {
                const final_entry = self.membership_state.finalizeChange(
                    self.current_term,
                    self.log.lastIndex() + 1,
                );
                if (final_entry) |entry| {
                    self.log.append(&.{entry}) catch {};
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

    pub fn ready(self: *RaftNode) rd.Ready {
        // Collect committed entries.
        if (self.log.committed > self.log.applied) {
            const entries = self.log.slice(
                self.log.applied + 1,
                self.log.committed + 1,
            ) catch &[_]rpc.Entry{};

            self.ready_state.addCommittedEntries(entries) catch {};

            // Free the slice allocated by log.slice() — entries have been copied into ready_state.
            if (entries.len > 0) {
                self.allocator.free(entries);
            }
        }

        // Check shouldSnapshot.
        if (self.snapshot_state.shouldSnapshot(self.log.lastIndex())) {
            self.ready_state.setShouldSnapshot(true);
        }

        return self.ready_state.ready(self.currentHardState());
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

        // Mark committed entries as applied.
        if (self.log.committed > self.log.applied) {
            self.log.appliedTo(self.log.committed);
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

        // Initialize replication_state with all peers.
        self.replication_state = try repl.ReplicationState.init(
            self.allocator,
            self.config.peers,
            self.log.lastIndex(),
        );

        self.election_state.resetHeartbeatElapsed();

        // Append no-op entry (critical for correctness, section 2.7).
        const noop = rpc.Entry{
            .index = self.log.lastIndex() + 1,
            .term = self.current_term,
            .kind = .noop,
            .data = &.{},
        };
        try self.log.append(&.{noop});
    }

    fn startElection(self: *RaftNode, pre_vote: bool) !void {
        if (pre_vote) {
            self.is_pre_vote_election = true;
            // Don't increment term or set voted_for for pre-vote.
        } else {
            self.current_term += 1;
            self.voted_for = self.id;
            self.is_pre_vote_election = false;
        }

        self.role = .candidate;
        self.election_state.resetElectionElapsed();
        self.election_state.resetElectionTimeout();

        // Reset votes, record own vote.
        self.election_state.resetVotes();
        try self.election_state.recordVote(self.id, true);

        // Single-node cluster: become leader immediately.
        if (self.config.peers.len == 0) {
            if (pre_vote) {
                // Pre-vote succeeded (self-only), start real election.
                try self.startElection(false);
            } else {
                try self.becomeLeader();
            }
            return;
        }

        // Build and send RequestVote to all peers.
        const request_term = if (pre_vote) self.current_term + 1 else self.current_term;
        const msg = rpc.Message{ .request_vote = .{
            .term = request_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
            .pre_vote = pre_vote,
        } };

        for (self.config.peers) |peer| {
            self.sendMsg(peer, msg);
        }
    }

    fn handleRequestVote(self: *RaftNode, from: u64, msg: rpc.RequestVote) !void {
        _ = from;

        // If msg.term > current_term and not a pre-vote, step down.
        if (msg.term > self.current_term and !msg.pre_vote) {
            self.becomeFollower(msg.term, null);
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

        try self.election_state.recordVote(from, msg.vote_granted);

        // Check quorum.
        const cfg = self.membership_state.config();
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

        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, null);
            return;
        }

        var rs = &(self.replication_state orelse return);
        const progress = rs.peers.getPtr(from) orelse return;

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

        const result = self.snapshot_state.receiveChunk(msg) catch {
            self.sendMsg(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = false,
            } });
            return;
        };

        if (result) |snapshot_val| {
            self.ready_state.setSnapshot(snapshot_val);
            self.sendMsg(msg.leader_id, rpc.Message{ .install_snapshot_response = .{
                .term = self.current_term,
                .success = true,
            } });
        }
    }

    fn handleInstallSnapshotResponse(self: *RaftNode, from: u64, msg: rpc.InstallSnapshotResponse) !void {
        _ = from;
        if (self.role != .leader) return;

        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, null);
        }
    }

    fn sendHeartbeats(self: *RaftNode) !void {
        for (self.config.peers) |peer| {
            if (self.replication_state) |*rs| {
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
                    // Peer needs snapshot; send empty heartbeat.
                    self.sendMsg(peer, rpc.Message{ .append_entries = .{
                        .term = self.current_term,
                        .leader_id = self.id,
                        .prev_log_index = 0,
                        .prev_log_term = 0,
                        .entries = &.{},
                        .leader_commit = self.log.committed,
                    } });
                }
            }
        }
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
            );
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
        try node.tick();
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
        try node.tick();
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
        try node.tick();
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
        try node.tick();
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
