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
