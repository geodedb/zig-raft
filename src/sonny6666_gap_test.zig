// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6666: a consumer may release its node lock between `ready()` and
//! `advancePersist()` (to sync a staged snapshot off the lock), so other
//! threads may step the node in that gap. Driven through real RaftNodes on
//! FileStorage under the leak-checking allocator.
//!
//! 1. advanceApply() marked applied up to the commit index at that moment,
//!    passing entries committed in the gap that no Ready delivered.
//! 2. ready() lent out entries still in the unstable log; a snapshot
//!    installed in the gap freed them under the consumer.
//! 3. Entries appended in the gap after a snapshot installed in the gap
//!    were written behind the older snapshot, then dropped from storage
//!    when the newer one was saved, while the node counted them durable.
//! 4. A follower whose log had committed past a snapshot's index installed
//!    it anyway, moving its commit and apply indexes backwards.
//! 5. RaftNode.init read the whole snapshot (and refused one over
//!    FileStorage.max_snapshot_size) to learn its index, term and config.

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");
const fstg = @import("file_storage.zig");

const RaftNode = node_mod.RaftNode;
const FileStorage = fstg.FileStorage;
const alloc = std.testing.allocator;

const Store = struct {
    tmp: std.testing.TmpDir,
    path: []const u8,
    fs: FileStorage,

    fn init(self: *Store) !void {
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/raft", .{self.tmp.sub_path});
        errdefer alloc.free(self.path);
        self.fs = try FileStorage.init(alloc, self.path);
    }

    fn reopen(self: *Store) !void {
        self.fs.deinit();
        self.fs = try FileStorage.init(alloc, self.path);
    }

    fn deinit(self: *Store) void {
        self.fs.deinit();
        alloc.free(self.path);
        self.tmp.cleanup();
    }
};

const cfg3 = rpc.ClusterConfig{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false };

fn follower(store: *Store) !RaftNode {
    return RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
}

fn ae(prev_index: u64, prev_term: u64, entries: []const rpc.Entry, commit: u64) rpc.Message {
    return .{ .append_entries = .{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .entries = entries,
        .leader_commit = commit,
    } };
}

fn entry(index: u64, data: []const u8) rpc.Entry {
    return .{ .index = index, .term = 1, .kind = .normal, .data = data };
}

fn install(index: u64, data: []const u8) rpc.Message {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(data, &digest, .{});
    return .{ .install_snapshot = .{
        .term = 1,
        .leader_id = 1,
        .last_included_index = index,
        .last_included_term = 1,
        .offset = 0,
        .data = data,
        .done = true,
        .checksum = digest,
        .config = cfg3,
    } };
}

fn drain(n: *RaftNode) !void {
    _ = try n.ready();
    try n.advance();
}

test "SONNY-6666: entries committed between ready() and advancePersist() are delivered, not skipped" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try follower(&store);
    defer n.deinit();

    try n.step(1, ae(0, 0, &.{ entry(1, "one"), entry(2, "two") }, 2));
    const rd = try n.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);

    // The consumer released its lock; another thread steps the node.
    try n.step(1, ae(2, 1, &.{entry(3, "three")}, 3));

    try n.advance();
    try std.testing.expectEqual(@as(u64, 2), n.log.applied);
    const rd2 = try n.ready();
    try std.testing.expectEqual(@as(usize, 1), rd2.committed_entries.len);
    try std.testing.expectEqualStrings("three", rd2.committed_entries[0].data);
    try n.advance();
    try std.testing.expectEqual(@as(u64, 3), n.log.applied);
}

test "SONNY-6666: a snapshot installed between ready() and advancePersist() does not free the Ready's entries" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try follower(&store);
    defer n.deinit();

    // Appended and committed in one message: still unstable when ready()
    // hands them out.
    try n.step(1, ae(0, 0, &.{ entry(1, "first-entry"), entry(2, "second-entry") }, 2));
    const rd = try n.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);

    try n.step(1, install(10, "snapshot-at-10"));

    // The consumer is still reading the Ready it was handed.
    try std.testing.expectEqualStrings("first-entry", rd.committed_entries[0].data);
    try std.testing.expectEqualStrings("second-entry", rd.committed_entries[1].data);
    try n.advance();

    const rd2 = try n.ready();
    try std.testing.expectEqual(@as(u64, 10), rd2.snapshot.?.metadata.index);
    try n.advance();
    try std.testing.expectEqual(@as(u64, 10), n.log.applied);
}

test "SONNY-6666: entries appended after a snapshot installed in the gap are persisted after it" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    {
        var n = try follower(&store);
        defer n.deinit();

        try n.step(1, ae(0, 0, &.{ entry(1, "a"), entry(2, "b") }, 2));
        _ = try n.ready();
        // In the gap: a snapshot at 10, then entries after it.
        try n.step(1, install(10, "snapshot-at-10"));
        try n.step(1, ae(10, 1, &.{ entry(11, "eleven"), entry(12, "twelve") }, 12));
        try n.advance();
        try drain(&n);
        try drain(&n);

        try std.testing.expectEqual(@as(u64, 12), n.log.lastIndex());
        try std.testing.expectEqual(@as(u64, 12), n.log.stableLastIndex());
        const got = try store.fs.storage().entries(11, 13);
        defer rpc.freeEntries(alloc, got);
        try std.testing.expectEqualStrings("eleven", got[0].data);
        try std.testing.expectEqualStrings("twelve", got[1].data);
    }

    try store.reopen();
    try std.testing.expectEqual(@as(u64, 10), store.fs.snapshot_index);
    try std.testing.expectEqual(@as(u64, 12), try store.fs.lastIndexImpl());
    var n = try follower(&store);
    defer n.deinit();
    try std.testing.expectEqual(@as(u64, 12), n.log.lastIndex());
}

test "SONNY-6666: a follower committed past a snapshot acks it without moving backwards" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try follower(&store);
    defer n.deinit();

    var ents: [8]rpc.Entry = undefined;
    for (&ents, 0..) |*e, i| e.* = entry(i + 1, "x");
    try n.step(1, ae(0, 0, &ents, 8));
    try drain(&n);
    try std.testing.expectEqual(@as(u64, 8), n.log.applied);

    try n.step(1, install(6, "snapshot-at-6"));
    const rd = try n.ready();
    try std.testing.expect(rd.snapshot == null);
    var acked = false;
    for (rd.messages) |out| if (out.msg == .install_snapshot_response) {
        try std.testing.expect(out.msg.install_snapshot_response.success);
        try std.testing.expectEqual(@as(u64, 8), out.msg.install_snapshot_response.last_index);
        acked = true;
    };
    try std.testing.expect(acked);
    try n.advance();
    try std.testing.expectEqual(@as(u64, 8), n.log.committed);
    try std.testing.expectEqual(@as(u64, 8), n.log.applied);
    try std.testing.expectEqual(@as(u64, 0), n.stats().snapshots_installed);
}

/// Tracks the largest single allocation.
const PeakAllocator = struct {
    child: std.mem.Allocator,
    largest: usize = 0,

    fn allocator(self: *PeakAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocFn, .resize = resizeFn, .remap = remapFn, .free = freeFn } };
    }
    fn allocFn(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.largest = @max(self.largest, len);
        return self.child.rawAlloc(len, a, ra);
    }
    fn resizeFn(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) bool {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.largest = @max(self.largest, n);
        return self.child.rawResize(m, a, n, ra);
    }
    fn remapFn(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.largest = @max(self.largest, n);
        return self.child.rawRemap(m, a, n, ra);
    }
    fn freeFn(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(m, a, ra);
    }
};

test "SONNY-6666: RaftNode.init reads only the snapshot's metadata" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();

    // 300 MiB: over FileStorage's default max_snapshot_size (256 MiB), so a
    // full load is refused outright.
    const size: usize = 300 * 1024 * 1024;
    {
        const data = try alloc.alloc(u8, size);
        defer alloc.free(data);
        for (0..size / (1024 * 1024)) |mib| @memset(data[mib * 1024 * 1024 ..][0 .. 1024 * 1024], @truncate(mib));
        try store.fs.storage().saveSnapshot(.{ .metadata = .{ .index = 42, .term = 3, .config = cfg3 }, .data = data });
    }
    // Reopen with every allocation (storage and node) tracked.
    var peak = PeakAllocator{ .child = alloc };
    store.fs.deinit();
    store.fs = try FileStorage.init(peak.allocator(), store.path);
    var n = try RaftNode.init(peak.allocator(), .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer n.deinit();
    try std.testing.expectEqual(@as(u64, 42), n.log.snapshot_index);
    try std.testing.expectEqual(@as(u64, 3), n.log.snapshot_term);
    try std.testing.expectEqual(@as(u64, 42), n.log.applied);
    try std.testing.expect(peak.largest < 1024 * 1024);
}
