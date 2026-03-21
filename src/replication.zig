// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");
const raft_log = @import("log.zig");
const quorum_mod = @import("quorum.zig");
const stg = @import("storage.zig");

pub const PeerProgress = struct {
    next_index: u64,
    match_index: u64,

    /// After rejection with conflict hints, update next_index for fast backtracking.
    /// Returns true if next_index changed.
    pub fn maybeDecrTo(
        self: *PeerProgress,
        rejected_index: u64,
        conflict_index: u64,
        conflict_term: u64,
    ) bool {
        _ = rejected_index;
        _ = conflict_term;
        const new_next = @max(conflict_index, 1);
        if (new_next == self.next_index) return false;
        self.next_index = new_next;
        return true;
    }

    /// After successful replication, update match_index and next_index.
    /// Returns true if match_index changed.
    pub fn maybeUpdate(self: *PeerProgress, match: u64) bool {
        var changed = false;
        if (match > self.match_index) {
            self.match_index = match;
            changed = true;
        }
        const desired_next = match + 1;
        if (desired_next > self.next_index) {
            self.next_index = desired_next;
        }
        return changed;
    }

    /// Returns true if this peer needs entries (next_index <= last_index).
    pub fn needsEntries(self: PeerProgress, last_index: u64) bool {
        return self.next_index <= last_index;
    }
};

pub const ReplicationState = struct {
    peers: std.AutoHashMap(u64, PeerProgress),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, peer_ids: []const u64, last_index: u64) !ReplicationState {
        var rs = ReplicationState{
            .peers = std.AutoHashMap(u64, PeerProgress).init(allocator),
            .allocator = allocator,
        };
        for (peer_ids) |pid| {
            try rs.peers.put(pid, .{
                .next_index = last_index + 1,
                .match_index = 0,
            });
        }
        return rs;
    }

    pub fn deinit(self: *ReplicationState) void {
        self.peers.deinit();
    }

    /// Reset all peers' next_index to last_index + 1, match_index to 0 (on becoming leader).
    pub fn resetPeers(self: *ReplicationState, peer_ids: []const u64, last_index: u64) !void {
        self.peers.clearRetainingCapacity();
        for (peer_ids) |pid| {
            try self.peers.put(pid, .{
                .next_index = last_index + 1,
                .match_index = 0,
            });
        }
    }

    /// Build an AppendEntries message for a specific peer.
    /// Returns null if the peer needs a snapshot (next_index <= snapshot_index).
    pub fn buildAppendEntries(
        self: *ReplicationState,
        peer_id: u64,
        raft_log_ptr: *raft_log.RaftLog,
        term: u64,
        leader_id: u64,
        commit_index: u64,
        max_entries: u32,
        max_bytes: u64,
    ) !?rpc.Message {
        const progress = self.peers.get(peer_id) orelse return null;
        const next = progress.next_index;
        const prev_log_index = next - 1;

        // Get prev_log_term; if compacted, peer needs a snapshot.
        const prev_log_term = raft_log_ptr.term(prev_log_index) catch |err| {
            if (err == stg.StorageError.Compacted) return null;
            return err;
        };

        // Compute the range of entries to send.
        const last = raft_log_ptr.lastIndex();
        const hi = @min(next + @as(u64, max_entries), last + 1);

        // Get entries from the log.
        const entries = raft_log_ptr.slice(next, hi) catch |err| {
            if (err == stg.StorageError.Compacted) return null;
            return err;
        };

        // Apply max_bytes limit.
        var total_bytes: u64 = 0;
        var count: usize = 0;
        for (entries) |entry| {
            total_bytes += entry.data.len;
            count += 1;
            if (total_bytes > max_bytes and count > 1) {
                count -= 1;
                break;
            }
        }

        const trimmed = entries[0..count];

        // Free excess if we trimmed.
        if (count < entries.len) {
            self.allocator.free(entries);
            const result = try self.allocator.alloc(rpc.Entry, trimmed.len);
            @memcpy(result, trimmed);
            return rpc.Message{ .append_entries = .{
                .term = term,
                .leader_id = leader_id,
                .prev_log_index = prev_log_index,
                .prev_log_term = prev_log_term,
                .entries = result,
                .leader_commit = commit_index,
            } };
        }

        return rpc.Message{ .append_entries = .{
            .term = term,
            .leader_id = leader_id,
            .prev_log_index = prev_log_index,
            .prev_log_term = prev_log_term,
            .entries = entries,
            .leader_commit = commit_index,
        } };
    }

    /// Compute new commit index based on majority match_index.
    /// Returns true if commit index advanced.
    pub fn maybeCommit(
        self: *ReplicationState,
        raft_log_ptr: *raft_log.RaftLog,
        voters: []const u64,
        voters_outgoing: []const u64,
        current_term: u64,
        leader_id: u64,
    ) bool {
        // Build parallel arrays of node_ids and match_indices.
        // Include the leader itself (match = lastIndex).
        var node_ids: [16]u64 = undefined;
        var match_indices: [16]u64 = undefined;
        var n: usize = 0;

        // Leader's own match index is its last log index.
        node_ids[n] = leader_id;
        match_indices[n] = raft_log_ptr.lastIndex();
        n += 1;

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            node_ids[n] = kv.key_ptr.*;
            match_indices[n] = kv.value_ptr.match_index;
            n += 1;
        }

        const new_commit = quorum_mod.committedIndex(
            voters,
            voters_outgoing,
            match_indices[0..n],
            node_ids[0..n],
            raft_log_ptr.committed,
        );

        if (new_commit <= raft_log_ptr.committed) return false;

        // Safety check (section 5.4.2): only commit entries from the current term.
        const entry_term = raft_log_ptr.term(new_commit) catch return false;
        if (entry_term != current_term) return false;

        raft_log_ptr.commitTo(new_commit);
        return true;
    }
};

// --- Tests ---

test "PeerProgress.maybeUpdate advances match_index and next_index" {
    var pp = PeerProgress{ .next_index = 1, .match_index = 0 };

    // Advance to match 5.
    try std.testing.expect(pp.maybeUpdate(5));
    try std.testing.expectEqual(@as(u64, 5), pp.match_index);
    try std.testing.expectEqual(@as(u64, 6), pp.next_index);

    // Same match should not change.
    try std.testing.expect(!pp.maybeUpdate(3));
    try std.testing.expectEqual(@as(u64, 5), pp.match_index);
    try std.testing.expectEqual(@as(u64, 6), pp.next_index);

    // Higher match advances again.
    try std.testing.expect(pp.maybeUpdate(10));
    try std.testing.expectEqual(@as(u64, 10), pp.match_index);
    try std.testing.expectEqual(@as(u64, 11), pp.next_index);
}

test "PeerProgress.maybeDecrTo backs off next_index using conflict hints" {
    var pp = PeerProgress{ .next_index = 10, .match_index = 0 };

    // Conflict at index 5, term 2 -- should jump to 5.
    try std.testing.expect(pp.maybeDecrTo(9, 5, 2));
    try std.testing.expectEqual(@as(u64, 5), pp.next_index);

    // Conflict with term 0 (log too short) -- jump to conflict_index.
    try std.testing.expect(pp.maybeDecrTo(4, 3, 0));
    try std.testing.expectEqual(@as(u64, 3), pp.next_index);

    // Same value should not change.
    try std.testing.expect(!pp.maybeDecrTo(2, 3, 0));
    try std.testing.expectEqual(@as(u64, 3), pp.next_index);

    // Never go below 1.
    try std.testing.expect(pp.maybeDecrTo(2, 0, 0));
    try std.testing.expectEqual(@as(u64, 1), pp.next_index);
}

test "ReplicationState.init peers initialized with correct next_index" {
    const peer_ids = [_]u64{ 2, 3, 4 };
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 10);
    defer rs.deinit();

    for (peer_ids) |pid| {
        const progress = rs.peers.get(pid).?;
        try std.testing.expectEqual(@as(u64, 11), progress.next_index);
        try std.testing.expectEqual(@as(u64, 0), progress.match_index);
    }
}

test "buildAppendEntries builds correct message with entries" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage());
    defer log.deinit();

    // Peer 2 needs entries starting at 1.
    const peer_ids = [_]u64{2};
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 0);
    defer rs.deinit();

    const msg = try rs.buildAppendEntries(2, &log, 2, 1, 0, 10, 4096);
    try std.testing.expect(msg != null);

    const ae = msg.?.append_entries;
    defer std.testing.allocator.free(ae.entries);

    try std.testing.expectEqual(@as(u64, 2), ae.term);
    try std.testing.expectEqual(@as(u64, 1), ae.leader_id);
    try std.testing.expectEqual(@as(u64, 0), ae.prev_log_index);
    try std.testing.expectEqual(@as(u64, 0), ae.prev_log_term);
    try std.testing.expectEqual(@as(usize, 3), ae.entries.len);
    try std.testing.expectEqual(@as(u64, 1), ae.entries[0].index);
    try std.testing.expectEqual(@as(u64, 3), ae.entries[2].index);
}

test "buildAppendEntries returns null when peer needs snapshot" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    // Compact up to index 2 so entries 1-2 are gone.
    try mem_store.compactImpl(2);

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage());
    defer log.deinit();

    // Peer 2 with next_index = 1 (needs entries starting before snapshot).
    const peer_ids = [_]u64{2};
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 0);
    defer rs.deinit();

    const msg = try rs.buildAppendEntries(2, &log, 2, 1, 0, 10, 4096);
    try std.testing.expect(msg == null);
}

test "maybeCommit advances commit when majority has replicated" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage());
    defer log.deinit();

    // Leader (node 1) appends entries at term 3.
    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 3, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);

    const peer_ids = [_]u64{ 2, 3 };
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 3);
    defer rs.deinit();

    // Peer 2 has replicated up to index 3, peer 3 up to index 1.
    _ = rs.peers.getPtr(2).?.maybeUpdate(3);
    _ = rs.peers.getPtr(3).?.maybeUpdate(1);

    const voters = [_]u64{ 1, 2, 3 };
    const committed = rs.maybeCommit(&log, &voters, &.{}, 3, 1);
    try std.testing.expect(committed);
    try std.testing.expectEqual(@as(u64, 3), log.committed);
}

test "maybeCommit does NOT commit entries from prior term (section 5.4.2)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage());
    defer log.deinit();

    // Entries from a previous term (term 2), current leader is term 3.
    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 2, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 2, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);

    const peer_ids = [_]u64{ 2, 3 };
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 2);
    defer rs.deinit();

    // Both peers have replicated everything.
    _ = rs.peers.getPtr(2).?.maybeUpdate(2);
    _ = rs.peers.getPtr(3).?.maybeUpdate(2);

    const voters = [_]u64{ 1, 2, 3 };
    // Current term is 3, but all entries are from term 2 -- must NOT commit.
    const committed = rs.maybeCommit(&log, &voters, &.{}, 3, 1);
    try std.testing.expect(!committed);
    try std.testing.expectEqual(@as(u64, 0), log.committed);
}
