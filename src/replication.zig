// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");
const raft_log = @import("log.zig");
const quorum_mod = @import("quorum.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");

pub const PeerProgress = struct {
    next_index: u64,
    match_index: u64,
    /// Index of the snapshot currently being installed on this peer, or 0 when
    /// no InstallSnapshot is in flight. Set by the leader when it escalates to
    /// InstallSnapshot (the peer's needed entries were compacted), and cleared
    /// when an InstallSnapshotResponse arrives. While non-zero the driver
    /// throttles re-sends so it does not re-ship the whole snapshot every
    /// heartbeat (SEC-0144).
    pending_snapshot_index: u64 = 0,
    /// Heartbeat rounds elapsed since the in-flight snapshot was (re)sent.
    /// Used to bound the re-send retry interval while `pending_snapshot_index`
    /// is non-zero, so a lost InstallSnapshot eventually retries (SEC-0144).
    pending_snapshot_ticks: u32 = 0,
    /// Consecutive InstallSnapshot refusals from this peer (SONNY-6636). Each
    /// doubles the wait before the snapshot is sent again (a refusal is
    /// usually not transient: a digest the follower cannot match, a store
    /// that cannot take it); an install resets it.
    snapshot_refusals: u32 = 0,

    /// Heartbeat rounds to wait before re-sending the in-flight snapshot:
    /// `base` while it was not refused, then `base << refusals`, at most
    /// `max` (SONNY-6636).
    pub fn snapshotResendTicks(self: *const PeerProgress, base: u32, max: u32) u32 {
        if (self.snapshot_refusals == 0) return base;
        const shift: u5 = @intCast(@min(self.snapshot_refusals, 16));
        const wait = @as(u64, base) << shift;
        return @intCast(@min(wait, @as(u64, max)));
    }

    /// True when the entries this peer still needs have been compacted away
    /// (next_index <= snapshot_index), so AppendEntries can never catch it up
    /// and the leader must escalate to InstallSnapshot (SEC-0144).
    pub fn needsSnapshot(self: *const PeerProgress, snapshot_index: u64) bool {
        if (snapshot_index == 0) return false;
        return self.next_index <= snapshot_index;
    }

    /// After a rejection of the AppendEntries whose prev_log_index was
    /// `rejected_index`, move next_index back to `conflict_index` (fast
    /// backtracking: use `backtrackHint` to fold the follower's
    /// `conflict_term` into it). Returns true if next_index changed.
    ///
    /// - A stale rejection (sent before next_index last moved:
    ///   `rejected_index + 1 != next_index`) and a hint past the rejected
    ///   index (the follower already holds that much, e.g. in its snapshot)
    ///   are ignored (SONNY-6611).
    /// - A rejection without a position (`rejected_index` 0: a stale-term
    ///   or malformed AppendEntries, a follower on raft <= 0.2.1, or a
    ///   transport that does not carry the field) backs off to
    ///   `conflict_index` as before, so mixed-version clusters still
    ///   converge.
    /// - Only allows backward movement of next_index to prevent a malicious
    ///   conflict_index from jumping next_index forward (SEC-0076).
    /// - Never moves next_index to or below match_index: the peer acked
    ///   those entries. Acting on such a rejection sent a peer that had just
    ///   installed a snapshot back below the snapshot, re-sending it forever.
    pub fn maybeDecrTo(
        self: *PeerProgress,
        rejected_index: u64,
        conflict_index: u64,
        conflict_term: u64,
    ) bool {
        _ = conflict_term;
        if (rejected_index != 0) {
            if (rejected_index +| 1 != self.next_index) return false;
            if (conflict_index > rejected_index) return false;
        }
        const floor = std.math.add(u64, self.match_index, 1) catch self.match_index;
        const new_next = @max(conflict_index, floor, 1);
        // Only allow backward movement (SEC-0076).
        if (new_next >= self.next_index) return false;
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
        // Checked overflow: match + 1 wraps on u64::MAX (SEC-0055).
        const desired_next = std.math.add(u64, match, 1) catch match;
        if (desired_next > self.next_index) {
            self.next_index = desired_next;
        }
        return changed;
    }
};

/// Where to resume after a follower rejected the AppendEntries whose
/// prev_log_index was `rejected_index`, reporting its `conflict_term` and
/// the first index of that term (`conflict_index`). If the leader holds
/// `conflict_term` at or below `rejected_index`, resume just past its last
/// entry of that term; otherwise skip the follower's whole term
/// (`conflict_index`). A term of 0 means "log too short": resume at
/// `conflict_index` (SONNY-6611).
pub fn backtrackHint(
    raft_log_ptr: *raft_log.RaftLog,
    rejected_index: u64,
    conflict_index: u64,
    conflict_term: u64,
) u64 {
    if (conflict_term == 0) return conflict_index;
    var i = @min(rejected_index, raft_log_ptr.lastIndex());
    while (i > raft_log_ptr.snapshot_index) : (i -= 1) {
        const t = raft_log_ptr.term(i) catch break;
        if (t == conflict_term) return i + 1;
        if (t < conflict_term) break;
    }
    return conflict_index;
}

pub const ReplicationState = struct {
    peers: std.AutoHashMap(u64, PeerProgress),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, peer_ids: []const u64, last_index: u64) !ReplicationState {
        var rs = ReplicationState{
            .peers = std.AutoHashMap(u64, PeerProgress).init(allocator),
            .allocator = allocator,
        };
        // Checked overflow: last_index + 1 (SEC-0070).
        const next_index = std.math.add(u64, last_index, 1) catch last_index;
        for (peer_ids) |pid| {
            try rs.peers.put(pid, .{
                .next_index = next_index,
                .match_index = 0,
            });
        }
        return rs;
    }

    pub fn deinit(self: *ReplicationState) void {
        self.peers.deinit();
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
        // Guard against underflow when next_index is 0 (SEC-0092).
        if (next == 0) return null;
        const prev_log_index = next - 1;

        // Get prev_log_term; if compacted, peer needs a snapshot.
        const prev_log_term = raft_log_ptr.term(prev_log_index) catch |err| {
            if (err == stg.StorageError.Compacted) return null;
            return err;
        };

        // Compute the range of entries to send (SEC-0070: checked overflow).
        const last = raft_log_ptr.lastIndex();

        // Short-circuit: peer is caught up — send empty heartbeat without
        // touching log.slice() or log.term() for the entry range (PERF-018).
        if (next > last) {
            return rpc.Message{
                .append_entries = .{
                    .term = term,
                    .leader_id = leader_id,
                    .prev_log_index = prev_log_index,
                    .prev_log_term = prev_log_term,
                    .entries = &.{},
                    .leader_commit = commit_index,
                },
            };
        }

        const next_plus = std.math.add(u64, next, @as(u64, max_entries)) catch std.math.maxInt(u64);
        const last_plus = std.math.add(u64, last, 1) catch std.math.maxInt(u64);
        const hi = @min(next_plus, last_plus);

        // Get entries from the log.
        const entries = raft_log_ptr.slice(next, hi) catch |err| {
            if (err == stg.StorageError.Compacted) return null;
            return err;
        };

        // Apply max_bytes limit.
        // Use checked addition to prevent u64 wraparound (SEC-0104).
        var total_bytes: u64 = 0;
        var count: usize = 0;
        for (entries) |entry| {
            total_bytes = std.math.add(u64, total_bytes, entry.data.len) catch {
                // Overflow means we've exceeded any representable limit — stop here.
                break;
            };
            count += 1;
            if (total_bytes > max_bytes and count > 1) {
                count -= 1;
                break;
            }
        }

        const trimmed = entries[0..count];

        // Free excess if we trimmed — allocate result BEFORE freeing source
        // to avoid use-after-free (trimmed points into entries).
        if (count < entries.len) {
            const result = self.allocator.alloc(rpc.Entry, trimmed.len) catch |err| {
                // Free the original entries, data included, on alloc failure
                // (SEC-0059, SONNY-6592).
                rpc.freeEntries(self.allocator, entries);
                return err;
            };
            @memcpy(result, trimmed);
            // The kept entries' data moved into `result`; the dropped ones'
            // data goes with the old slice (SONNY-6592).
            for (entries[count..]) |e| if (e.data.len > 0) self.allocator.free(e.data);
            self.allocator.free(entries);
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
    ) !bool {
        // Build parallel arrays of node_ids and match_indices.
        // Include the leader itself (match = lastIndex).
        // Use stack buffers for typical cluster sizes (PERF-001).
        const count = self.peers.count() + 1;
        var nid_stack: [node_mod.MAX_STACK_PEERS]u64 = undefined;
        var mi_stack: [node_mod.MAX_STACK_PEERS]u64 = undefined;
        const node_ids = if (count <= 16)
            nid_stack[0..count]
        else
            try self.allocator.alloc(u64, count);
        defer if (count > 16) self.allocator.free(node_ids);
        const match_indices = if (count <= 16)
            mi_stack[0..count]
        else
            try self.allocator.alloc(u64, count);
        defer if (count > 16) self.allocator.free(match_indices);
        var n: usize = 0;

        // The leader counts itself only for entries it has persisted: an
        // unsaved entry could be lost with the leader yet already count
        // toward commit (SONNY-6611).
        node_ids[n] = leader_id;
        match_indices[n] = raft_log_ptr.stableLastIndex();
        n += 1;

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            node_ids[n] = kv.key_ptr.*;
            match_indices[n] = kv.value_ptr.match_index;
            n += 1;
        }

        const new_commit = try quorum_mod.committedIndex(
            self.allocator,
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

    // Conflict at index 5, term 2 -- should back off to 5.
    try std.testing.expect(pp.maybeDecrTo(9, 5, 2));
    try std.testing.expectEqual(@as(u64, 5), pp.next_index);

    // Conflict with term 0 (log too short) -- back off to conflict_index.
    try std.testing.expect(pp.maybeDecrTo(4, 3, 0));
    try std.testing.expectEqual(@as(u64, 3), pp.next_index);

    // Same value should not change (SEC-0076: >= prevents forward jump).
    try std.testing.expect(!pp.maybeDecrTo(2, 3, 0));
    try std.testing.expectEqual(@as(u64, 3), pp.next_index);

    // Forward jump should be rejected (SEC-0076).
    try std.testing.expect(!pp.maybeDecrTo(2, 100, 0));
    try std.testing.expectEqual(@as(u64, 3), pp.next_index);

    // Never go below 1.
    try std.testing.expect(pp.maybeDecrTo(2, 0, 0));
    try std.testing.expectEqual(@as(u64, 1), pp.next_index);
}

test "PeerProgress.needsSnapshot detects compaction boundary (SEC-0144)" {
    // No snapshot taken yet: never needs a snapshot regardless of next_index.
    var fresh = PeerProgress{ .next_index = 1, .match_index = 0 };
    try std.testing.expect(!fresh.needsSnapshot(0));

    // next_index strictly above snapshot_index: catchable via AppendEntries.
    var ahead = PeerProgress{ .next_index = 11, .match_index = 9 };
    try std.testing.expect(!ahead.needsSnapshot(10));

    // Boundary: next_index == snapshot_index means the entry at next_index is
    // compacted, so the peer needs a snapshot.
    var at_boundary = PeerProgress{ .next_index = 10, .match_index = 0 };
    try std.testing.expect(at_boundary.needsSnapshot(10));

    // next_index below snapshot_index: definitely needs a snapshot.
    var behind = PeerProgress{ .next_index = 3, .match_index = 0 };
    try std.testing.expect(behind.needsSnapshot(10));

    // Defaults: in-flight markers start cleared.
    try std.testing.expectEqual(@as(u64, 0), behind.pending_snapshot_index);
    try std.testing.expectEqual(@as(u32, 0), behind.pending_snapshot_ticks);
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

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Peer 2 needs entries starting at 1.
    const peer_ids = [_]u64{2};
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 0);
    defer rs.deinit();

    const msg = try rs.buildAppendEntries(2, &log, 2, 1, 0, 10, 4096);
    try std.testing.expect(msg != null);

    const ae = msg.?.append_entries;
    defer rpc.freeEntries(std.testing.allocator, ae.entries);

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

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
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

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Leader (node 1) appends entries at term 3.
    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 3, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);
    // The leader has persisted them (advancePersist + stableTo).
    try mem_store.appendEntriesImpl(&entries);
    try log.stableTo(3, 3);

    const peer_ids = [_]u64{ 2, 3 };
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 3);
    defer rs.deinit();

    // Peer 2 has replicated up to index 3, peer 3 up to index 1.
    _ = rs.peers.getPtr(2).?.maybeUpdate(3);
    _ = rs.peers.getPtr(3).?.maybeUpdate(1);

    const voters = [_]u64{ 1, 2, 3 };
    const committed = try rs.maybeCommit(&log, &voters, &.{}, 3, 1);
    try std.testing.expect(committed);
    try std.testing.expectEqual(@as(u64, 3), log.committed);
}

test "maybeCommit does not count the leader's unpersisted entries (SONNY-6611)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 3, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 3, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries); // in the leader's log, not yet persisted

    const peer_ids = [_]u64{ 2, 3 };
    var rs = try ReplicationState.init(std.testing.allocator, &peer_ids, 3);
    defer rs.deinit();
    _ = rs.peers.getPtr(2).?.maybeUpdate(3);
    _ = rs.peers.getPtr(3).?.maybeUpdate(1);

    // Only peer 2 holds 2..3 durably: one of three, not a majority.
    const voters = [_]u64{ 1, 2, 3 };
    _ = try rs.maybeCommit(&log, &voters, &.{}, 3, 1);
    try std.testing.expectEqual(@as(u64, 1), log.committed);

    // Once the leader has persisted them, 3 is held by a majority.
    try mem_store.appendEntriesImpl(&entries);
    try log.stableTo(3, 3);
    try std.testing.expect(try rs.maybeCommit(&log, &voters, &.{}, 3, 1));
    try std.testing.expectEqual(@as(u64, 3), log.committed);
}

test "maybeCommit does NOT commit entries from prior term (section 5.4.2)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
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
    const committed = try rs.maybeCommit(&log, &voters, &.{}, 3, 1);
    try std.testing.expect(!committed);
    try std.testing.expectEqual(@as(u64, 0), log.committed);
}

test "maybeDecrTo ignores a stale rejection below the acked match_index" {
    // The peer acked through 10 (e.g. a snapshot install); a rejection of an
    // older AppendEntries that arrives late must not send it back below that.
    var pp = PeerProgress{ .next_index = 11, .match_index = 10 };
    try std.testing.expect(!pp.maybeDecrTo(5, 3, 1));
    try std.testing.expectEqual(@as(u64, 11), pp.next_index);
    try std.testing.expect(!pp.needsSnapshot(10));

    // A real rejection above match_index still backs off, down to match + 1.
    var probe = PeerProgress{ .next_index = 20, .match_index = 10 };
    try std.testing.expect(probe.maybeDecrTo(19, 15, 2));
    try std.testing.expectEqual(@as(u64, 15), probe.next_index);
    try std.testing.expect(probe.maybeDecrTo(14, 4, 1));
    try std.testing.expectEqual(@as(u64, 11), probe.next_index);
}

test "maybeDecrTo ignores duplicate rejections and hints past the rejected index (SONNY-6611)" {
    var pp = PeerProgress{ .next_index = 20, .match_index = 0 };
    try std.testing.expect(pp.maybeDecrTo(19, 12, 0));
    try std.testing.expectEqual(@as(u64, 12), pp.next_index);
    // The same rejection again (a second heartbeat sent from 20): stale.
    try std.testing.expect(!pp.maybeDecrTo(19, 12, 0));
    // A hint past the rejected index: the follower holds that much.
    try std.testing.expect(!pp.maybeDecrTo(11, 30, 0));
    try std.testing.expectEqual(@as(u64, 12), pp.next_index);
}

test "backtrackHint skips to the leader's last entry of the conflicting term (SONNY-6611)" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();
    var log = try raft_log.RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();
    var ents: [10]rpc.Entry = undefined;
    // Leader: 1..4 term 1, 5..7 term 2, 8..10 term 4.
    for (&ents, 0..) |*e, i| e.* = .{ .index = i + 1, .term = if (i < 4) 1 else if (i < 7) 2 else 4, .kind = .normal, .data = &.{} };
    try log.append(&ents);

    // Follower has term 2 at 9 (first at 5): leader's last term-2 entry is 7.
    try std.testing.expectEqual(@as(u64, 8), backtrackHint(&log, 9, 5, 2));
    // Follower has term 3 (first at 6), which the leader never had.
    try std.testing.expectEqual(@as(u64, 6), backtrackHint(&log, 9, 6, 3));
    // Log too short.
    try std.testing.expectEqual(@as(u64, 4), backtrackHint(&log, 9, 4, 0));
}

test "maybeDecrTo still backs off on a rejection without a position (mixed versions)" {
    // A follower on raft <= 0.2.1 (or a transport that drops the hints)
    // reports a rejection with match_index 0 and no conflict hint. The
    // leader must keep backing off, as before, or that follower stalls.
    var pp = PeerProgress{ .next_index = 20, .match_index = 6 };
    try std.testing.expect(pp.maybeDecrTo(0, 0, 0));
    try std.testing.expectEqual(@as(u64, 7), pp.next_index);
    // An old follower's hint (its rejected prev_log_index) is still used.
    var hinted = PeerProgress{ .next_index = 20, .match_index = 0 };
    try std.testing.expect(hinted.maybeDecrTo(0, 15, 2));
    try std.testing.expectEqual(@as(u64, 15), hinted.next_index);
}
