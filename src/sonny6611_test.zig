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
