// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6611 regressions, driven through real RaftNodes on real storage
//! (FileStorage / InMemoryStorage) under the leak-checking allocator.
//!
//! 1. A leader counted entries it had not yet persisted toward commit.
//! 2. A second ready() without advance() duplicated committed entries.
//! 3. A snapshot installed between ready() and advance() never reached the
//!    consumer (and was leaked).
//! 5. AppendEntries rejections backed next_index off one entry at a time.

const std = @import("std");
const rpc = @import("rpc.zig");
const st = @import("state.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");
const trans = @import("transport.zig");
const fstg = @import("file_storage.zig");

const RaftNode = node_mod.RaftNode;
const FileStorage = fstg.FileStorage;
const alloc = std.testing.allocator;

const TestStore = struct {
    tmp: std.testing.TmpDir,
    path: []const u8,
    fs: FileStorage,

    fn init(self: *TestStore) !void {
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/raft", .{self.tmp.sub_path});
        errdefer alloc.free(self.path);
        self.fs = try FileStorage.init(alloc, self.path);
    }

    fn deinit(self: *TestStore) void {
        self.fs.deinit();
        alloc.free(self.path);
        self.tmp.cleanup();
    }
};

fn drain(node: *RaftNode) !void {
    _ = try node.ready();
    try node.advance();
}

fn entriesRange(buf: []rpc.Entry, first: u64, term: u64) []rpc.Entry {
    for (buf, 0..) |*e, i| e.* = .{ .index = first + i, .term = term, .kind = .normal, .data = "payload" };
    return buf;
}

fn appendEntries(term: u64, leader: u64, prev_index: u64, prev_term: u64, entries: []const rpc.Entry, commit: u64) rpc.Message {
    return .{ .append_entries = .{
        .term = term,
        .leader_id = leader,
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .entries = entries,
        .leader_commit = commit,
    } };
}

test "SONNY-6611: a second ready() before advance() does not duplicate committed entries" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();

    const ents = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "alpha" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "bravo" },
    };
    try node.step(1, appendEntries(1, 1, 0, 0, &ents, 0));
    try drain(&node);
    try node.step(1, appendEntries(1, 1, 2, 1, &.{}, 2));

    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectError(error.ReadyNotAdvanced, node.ready());
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectEqualStrings("bravo", rd.committed_entries[1].data);
    try node.advance();

    try std.testing.expectEqual(@as(u64, 2), node.stats().entries_applied);
    const rd2 = try node.ready();
    try std.testing.expectEqual(@as(usize, 0), rd2.committed_entries.len);
    try node.advance();
}

// ---------------------------------------------------------------------------
// 3. A snapshot installed between ready() and advance().
// ---------------------------------------------------------------------------

test "SONNY-6611: a snapshot installed between ready() and advance() reaches the next Ready" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();

    const data = "snapshot-state";
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(data, &digest, .{});

    _ = try node.ready();
    try node.step(1, .{ .install_snapshot = .{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = data,
        .done = true,
        .checksum = digest,
        .config = .{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false },
    } });
    try node.advance();

    const rd = try node.ready();
    try std.testing.expect(rd.snapshot != null);
    try std.testing.expectEqual(@as(u64, 5), rd.snapshot.?.metadata.index);
    try std.testing.expectEqualStrings(data, rd.snapshot.?.data);
    try node.advancePersist();
    const saved = (try store.fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqual(@as(u64, 5), saved.metadata.index);
    try node.advanceApply();
}

// ---------------------------------------------------------------------------
// 1. Commit only what is durable.
// ---------------------------------------------------------------------------

test "SONNY-6611: a single-node leader commits an entry only once it is durable" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{}, .rng_seed = 5 }, store.fs.storage());
    defer node.deinit();

    var i: u32 = 0;
    while (i < 40 and node.role != .leader) : (i += 1) try node.tick(null);
    try std.testing.expectEqual(st.Role.leader, node.role);
    try drain(&node);
    try drain(&node);

    try node.propose("must-be-durable");
    const idx = node.log.lastIndex();
    try node.tick(null);
    // Not on disk yet, so not committed.
    try std.testing.expect(try store.fs.storage().lastIndex() < idx);
    try std.testing.expect(node.log.committed < idx);

    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 0), rd.committed_entries.len);
    try node.advancePersist();
    try std.testing.expectEqual(idx, try store.fs.storage().lastIndex());
    try node.advanceApply();
    // Durable now: committed without waiting for another tick.
    try std.testing.expectEqual(idx, node.log.committed);

    const rd2 = try node.ready();
    try std.testing.expectEqual(@as(usize, 1), rd2.committed_entries.len);
    try std.testing.expectEqualStrings("must-be-durable", rd2.committed_entries[0].data);
    try node.advance();
}

// ---------------------------------------------------------------------------
// 5. Fast backtracking.
// ---------------------------------------------------------------------------

/// Queues AppendEntries for peer 2 (deep copies), drops everything else.
const PeerTwoTransport = struct {
    queue: std.ArrayList(rpc.Message) = .empty,

    const vtable_inst = trans.Transport.VTable{ .send = &sendImpl };

    fn transport(self: *PeerTwoTransport) trans.Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_inst };
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: rpc.Message) void {
        const self: *PeerTwoTransport = @ptrCast(@alignCast(ptr));
        if (to != 2) return;
        var copy = msg;
        switch (msg) {
            .append_entries => |ae| copy.append_entries.entries = rpc.dupeEntries(alloc, ae.entries) catch return,
            else => return,
        }
        self.queue.append(alloc, copy) catch rpc.freeEntries(alloc, copy.append_entries.entries);
    }

    fn deinit(self: *PeerTwoTransport) void {
        for (self.queue.items) |m| rpc.freeEntries(alloc, m.append_entries.entries);
        self.queue.deinit(alloc);
    }
};

test "SONNY-6611: a diverged follower is caught up in a few rounds, not one entry per round" {
    // Follower 2: 1..40 at term 1 (from leader 3, term 1; never replicated).
    var store2 = stg.InMemoryStorage.init(alloc);
    defer store2.deinit();
    var n2 = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store2.storage());
    defer n2.deinit();
    var buf: [64]rpc.Entry = undefined;
    try n2.step(3, appendEntries(1, 3, 0, 0, entriesRange(buf[0..40], 1, 1), 0));
    try drain(&n2);

    // Node 1: 1..10 at term 1, then 11..60 at term 2 (leader 3, term 2).
    var store1 = stg.InMemoryStorage.init(alloc);
    defer store1.deinit();
    var n1 = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{ 2, 3 }, .pre_vote = false, .rng_seed = 9 }, store1.storage());
    defer n1.deinit();
    try n1.step(3, appendEntries(1, 3, 0, 0, entriesRange(buf[0..10], 1, 1), 0));
    try n1.step(3, appendEntries(2, 3, 10, 1, entriesRange(buf[0..50], 11, 2), 10));
    try drain(&n1);

    // Node 1 wins term 3; its next_index for node 2 starts at 61.
    var t: u32 = 0;
    while (t < 40 and n1.role != .candidate) : (t += 1) try n1.tick(null);
    try n1.step(3, .{ .request_vote_response = .{ .term = n1.current_term, .vote_granted = true, .pre_vote = false } });
    try std.testing.expectEqual(st.Role.leader, n1.role);
    try drain(&n1);

    var net: PeerTwoTransport = .{};
    defer net.deinit();
    n1.setTransport(net.transport());

    var rounds: u32 = 0;
    while (rounds < 100) : (rounds += 1) {
        if (n2.log.lastIndex() == n1.log.lastIndex() and (n2.log.term(n2.log.lastIndex()) catch 0) == n1.current_term) break;
        try n1.tick(null);
        const batch = try net.queue.toOwnedSlice(alloc);
        defer {
            for (batch) |m| rpc.freeEntries(alloc, m.append_entries.entries);
            alloc.free(batch);
        }
        for (batch) |m| {
            try n2.step(1, m);
            const rd = try n2.ready();
            try n2.advancePersist();
            for (rd.messages) |out| if (out.to == 1) try n1.step(2, out.msg);
            try n2.advanceApply();
        }
        try drain(&n1);
    }
    // Log too short (61 -> 41), then term 1 at 40 -> leader's last term-1
    // entry is 10 (-> 11), then a match: a handful of rounds.
    try std.testing.expect(rounds <= 5);
    try std.testing.expectEqual(n1.log.lastIndex(), n2.log.lastIndex());
}

// ---------------------------------------------------------------------------
// 7. A snapshot shrinks the stored log.
// ---------------------------------------------------------------------------

test "SONNY-6611: reportSnapshot drops the snapshotted entries from FileStorage, across a restart" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var applied: u64 = 0;
    {
        var node = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{}, .rng_seed = 5 }, store.fs.storage());
        defer node.deinit();
        var i: u32 = 0;
        while (i < 40 and node.role != .leader) : (i += 1) try node.tick(null);
        for (0..5) |_| try node.propose("before-snapshot");
        try drain(&node);
        try drain(&node);
        applied = node.log.applied;
        try std.testing.expect(applied >= 6);

        try node.reportSnapshot("state-machine-image");
        // Every stored entry was covered: the stored log is empty.
        try std.testing.expectEqual(@as(usize, 0), store.fs.index.items.len);
        try std.testing.expectEqual(applied, try store.fs.storage().lastIndex());

        try node.propose("after-snapshot");
        try drain(&node);
        try drain(&node);
        try std.testing.expectEqual(applied + 1, try store.fs.storage().lastIndex());
    }

    // Restart from disk.
    store.fs.deinit();
    store.fs = try FileStorage.init(alloc, store.path);
    var node = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{}, .rng_seed = 5 }, store.fs.storage());
    defer node.deinit();
    try std.testing.expectEqual(applied, node.log.snapshot_index);
    try std.testing.expectEqual(applied + 1, node.log.lastIndex());
    const tail = try node.log.slice(applied + 1, applied + 2);
    defer rpc.freeEntries(alloc, tail);
    try std.testing.expectEqualStrings("after-snapshot", tail[0].data);
}
