// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6666: a follower installing a snapshot assembled all of it in
//! memory, then wrote and fsynced it inside `advancePersist()`, under the
//! consumer's node lock. With `Config.stage_received_snapshots` it writes
//! each chunk to a staged file as it arrives, the consumer syncs the staged
//! file off the lock, and `advancePersist()` commits it with renames.
//!
//! These tests stream a real 1 GiB snapshot (`snapshot.streamSnapshot`, as
//! a leader does) into a follower on FileStorage, time every section that
//! runs under the lock, and take a copy of the follower's directory at
//! every step of an install to show what a crash there leaves.

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");
const fstg = @import("file_storage.zig");
const trans = @import("transport.zig");
const snap_mod = @import("snapshot.zig");

const RaftNode = node_mod.RaftNode;
const FileStorage = fstg.FileStorage;
const alloc = std.testing.allocator;

/// Geode's tick budget: no section under the lock may exceed it.
const LOCK_BUDGET_NS: u64 = 250 * std.time.ns_per_ms;
/// The commit (renames and directory syncs) stays well below it.
const COMMIT_BUDGET_NS: u64 = 100 * std.time.ns_per_ms;

const cfg3 = rpc.ClusterConfig{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false };

fn nowNs() u64 {
    return @intCast(std.Io.Clock.Timestamp.now(io(), .awake).raw.nanoseconds);
}

fn io() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

const Store = struct {
    tmp: std.testing.TmpDir,
    path: []const u8,
    fs: FileStorage,

    fn init(self: *Store, a: std.mem.Allocator) !void {
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/raft", .{self.tmp.sub_path});
        errdefer alloc.free(self.path);
        self.fs = try FileStorage.init(a, self.path);
    }

    fn deinit(self: *Store) void {
        self.fs.deinit();
        alloc.free(self.path);
        self.tmp.cleanup();
    }
};

fn countFiles(path: []const u8, prefix: []const u8) !usize {
    var dir = try std.Io.Dir.cwd().openDir(io(), path, .{ .iterate = true });
    defer dir.close(io());
    var it = dir.iterate();
    var n: usize = 0;
    while (try it.next(io())) |e| {
        if (std.mem.startsWith(u8, e.name, prefix)) n += 1;
    }
    return n;
}

/// Copy every file in `src` to a new directory `dst`: the disk as a crash
/// at this instant would leave it (every write so far reached the disk).
fn diskImage(src: []const u8, dst: []const u8) !void {
    var from = try std.Io.Dir.cwd().openDir(io(), src, .{ .iterate = true });
    defer from.close(io());
    var to = try std.Io.Dir.cwd().createDirPathOpen(io(), dst, .{});
    defer to.close(io());
    var it = from.iterate();
    while (try it.next(io())) |e| {
        if (e.kind != .file) continue;
        try std.Io.Dir.copyFile(from, e.name, to, e.name, io(), .{});
    }
}

/// A snapshot whose bytes are computed, so 1 GiB needs no 1 GiB buffer.
const PatternReader = struct {
    metadata: rpc.SnapshotMetadata,
    expected_digest: ?[32]u8 = null,
    size: u64,

    fn byteAt(offset: u64) u8 {
        return @truncate(offset ^ (offset >> 11) ^ (offset >> 23));
    }

    pub fn readAt(self: *PatternReader, buf: []u8, offset: u64) !usize {
        const n: usize = @intCast(@min(@as(u64, buf.len), self.size - offset));
        for (buf[0..n], 0..) |*b, i| b.* = byteAt(offset + i);
        return n;
    }
};

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

/// The consumer of a follower: every message is stepped under the "lock",
/// a staged snapshot in the Ready is synced with the lock released, and
/// the time of every locked section is recorded.
const LockedFollower = struct {
    node: *RaftNode,
    storage: stg.Storage,
    longest: u64 = 0,
    longest_commit: u64 = 0,
    sections: u64 = 0,
    staged_seen: u32 = 0,
    last_checksum: [32]u8 = .{0} ** 32,
    failure: ?anyerror = null,

    const vtable_inst = trans.Transport.VTable{ .send = &sendImpl };

    fn transport(self: *LockedFollower) trans.Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_inst };
    }

    fn record(self: *LockedFollower, t0: u64) u64 {
        const held = nowNs() - t0;
        self.longest = @max(self.longest, held);
        self.sections += 1;
        return held;
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: rpc.Message) void {
        const self: *LockedFollower = @ptrCast(@alignCast(ptr));
        std.debug.assert(to == self.node.id);
        if (msg == .install_snapshot and msg.install_snapshot.done) self.last_checksum = msg.install_snapshot.checksum;
        self.deliver(msg) catch |err| {
            if (self.failure == null) self.failure = err;
        };
    }

    fn deliver(self: *LockedFollower, msg: rpc.Message) !void {
        // Locked: step, then the Ready.
        var t0 = nowNs();
        try self.node.step(1, msg);
        const rd = try self.node.ready();
        _ = self.record(t0);

        if (rd.staged_snapshot) |staged| {
            // Unlocked: sync the staged snapshot.
            self.staged_seen += 1;
            try self.storage.syncStagedSnapshot(staged);
        }

        // Locked again: commit and finish the cycle.
        t0 = nowNs();
        try self.node.advancePersist();
        try self.node.advanceApply();
        const held = self.record(t0);
        if (rd.staged_snapshot != null) self.longest_commit = @max(self.longest_commit, held);
    }
};

test "SONNY-6666: a 1 GiB install never holds the follower's lock long, and never holds the snapshot in memory" {
    var peak = PeakAllocator{ .child = alloc };
    var store: Store = undefined;
    try store.init(peak.allocator());
    defer store.deinit();
    var n = try RaftNode.init(peak.allocator(), .{
        .id = 2,
        .peers = &.{ 1, 3 },
        .stage_received_snapshots = true,
        .max_snapshot_size = 0,
    }, store.fs.storage());
    defer n.deinit();

    const size: u64 = 1024 * 1024 * 1024;
    var reader = PatternReader{ .metadata = .{ .index = 100, .term = 1, .config = cfg3 }, .size = size };
    var consumer = LockedFollower{ .node = &n, .storage = store.fs.storage() };
    try snap_mod.streamSnapshot(alloc, &reader, .{
        .to = 2,
        .term = 1,
        .leader_id = 1,
        .index = 100,
        .offset = 0,
        .chunk_size = 1024 * 1024,
    }, consumer.transport());
    if (consumer.failure) |err| return err;

    std.log.info("SONNY-6666: 1 GiB install: {d} locked sections, longest {d} us, commit {d} us, largest allocation {d} B", .{
        consumer.sections, consumer.longest / 1000, consumer.longest_commit / 1000, peak.largest,
    });
    try std.testing.expectEqual(@as(u32, 1), consumer.staged_seen);
    try std.testing.expect(consumer.longest < LOCK_BUDGET_NS);
    try std.testing.expect(consumer.longest_commit < COMMIT_BUDGET_NS);
    // One chunk, not the snapshot.
    try std.testing.expect(peak.largest < 8 * 1024 * 1024);

    try std.testing.expectEqual(@as(u64, 100), n.log.snapshot_index);
    try std.testing.expectEqual(@as(u64, 100), n.log.applied);
    try std.testing.expectEqual(@as(u64, 1), n.stats().snapshots_installed);
    try std.testing.expectEqualSlices(u8, &consumer.last_checksum, &store.fs.snapshot_data_digest);
    try std.testing.expectEqual(@as(usize, 0), try countFiles(store.path, "snapshot.stage."));

    // Restart on it: metadata only, and the data reads back.
    n.deinit();
    store.fs.deinit();
    store.fs = try FileStorage.init(peak.allocator(), store.path);
    n = try RaftNode.init(peak.allocator(), .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    try std.testing.expectEqual(@as(u64, 100), n.log.snapshot_index);
    var r = (try store.fs.openSnapshotReader()).?;
    defer r.close();
    try std.testing.expectEqual(size, r.size);
    for ([_]u64{ 0, 4096, size / 2 + 7, size - 3 }) |off| {
        var got: [3]u8 = undefined;
        _ = try r.readAt(&got, off);
        var want: [3]u8 = undefined;
        _ = try reader.readAt(&want, off);
        try std.testing.expectEqualSlices(u8, &want, &got);
    }
}

// ---------------------------------------------------------------------------
// Crash at every step of an install.
// ---------------------------------------------------------------------------

const Step = enum {
    first_chunk,
    last_chunk,
    synced,
    commit_before_meta_rename,
    commit_after_meta_rename,
    commit_after_data_rename,
    persisted,
};

fn chunk(index: u64, offset: u64, data: []const u8, done: bool, whole: []const u8) rpc.Message {
    var digest: [32]u8 = .{0} ** 32;
    if (done) std.crypto.hash.Blake3.hash(whole, &digest, .{});
    return .{ .install_snapshot = .{
        .term = 1,
        .leader_id = 1,
        .last_included_index = index,
        .last_included_term = 1,
        .offset = offset,
        .data = data,
        .done = done,
        .checksum = digest,
        .config = cfg3,
    } };
}

const SNAP_A = "snapshot-A-at-index-5";
const SNAP_B = "snapshot-B-at-index-9-spans-three-chunks-of-the-stream" ** 40;

/// Install A (index 5) completely, then B (index 9) up to `step`, and copy
/// the directory to `image` at that instant.
fn installUpTo(step: Step, image: []const u8) !void {
    var store: Store = undefined;
    try store.init(alloc);
    defer store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 }, .stage_received_snapshots = true }, store.fs.storage());
    defer n.deinit();
    const storage = store.fs.storage();

    try n.step(1, chunk(5, 0, SNAP_A, true, SNAP_A));
    {
        const rd = try n.ready();
        try storage.syncStagedSnapshot(rd.staged_snapshot.?);
        try n.advance();
    }
    // Entries after A, so the log has something to keep or drop.
    try n.step(1, .{ .append_entries = .{ .term = 1, .leader_id = 1, .prev_log_index = 5, .prev_log_term = 1, .entries = &.{
        .{ .index = 6, .term = 1, .kind = .normal, .data = "six" },
        .{ .index = 7, .term = 1, .kind = .normal, .data = "seven" },
    }, .leader_commit = 7 } });
    _ = try n.ready();
    try n.advance();

    const third = SNAP_B.len / 3;
    try n.step(1, chunk(9, 0, SNAP_B[0..third], false, SNAP_B));
    if (step == .first_chunk) return diskImage(store.path, image);
    try n.step(1, chunk(9, third, SNAP_B[third .. 2 * third], false, SNAP_B));
    try n.step(1, chunk(9, 2 * third, SNAP_B[2 * third ..], true, SNAP_B));
    if (step == .last_chunk) return diskImage(store.path, image);

    const rd = try n.ready();
    try storage.syncStagedSnapshot(rd.staged_snapshot.?);
    if (step == .synced) return diskImage(store.path, image);

    store.fs.crash_at = switch (step) {
        .commit_before_meta_rename => .before_meta_rename,
        .commit_after_meta_rename => .after_meta_rename,
        .commit_after_data_rename => .after_data_rename,
        else => .none,
    };
    if (store.fs.crash_at != .none) {
        try std.testing.expectError(error.SimulatedCrash, n.advancePersist());
        return diskImage(store.path, image);
    }
    try n.advancePersist();
    try n.advanceApply();
    return diskImage(store.path, image);
}

fn expectRecovered(image: []const u8, index: u64, data: []const u8) !void {
    var fs = try FileStorage.init(alloc, image);
    defer fs.deinit();
    try std.testing.expectEqual(index, fs.snapshot_index);
    const saved = (try fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqual(index, saved.metadata.index);
    try std.testing.expectEqualStrings(data, saved.data);
    try std.testing.expectEqualSlices(u64, cfg3.voters, saved.metadata.config.voters);
    try std.testing.expectEqual(@as(usize, 0), try countFiles(image, "snapshot.stage."));
    try std.testing.expectEqual(@as(usize, 0), try countFiles(image, "snapshot.commit."));

    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, fs.storage());
    defer n.deinit();
    try std.testing.expectEqual(index, n.log.snapshot_index);
    // Before B: A plus entries 6..7. After B: B, whose index 9 is past them.
    try std.testing.expectEqual(if (index == 5) @as(u64, 7) else 9, n.log.lastIndex());
}

test "SONNY-6666: a crash at any step of an install leaves the previous snapshot or the new one, whole" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    inline for (std.meta.fields(Step)) |f| {
        const step: Step = @enumFromInt(f.value);
        const image = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, f.name });
        defer alloc.free(image);
        try installUpTo(step, image);
        switch (step) {
            .first_chunk, .last_chunk, .synced, .commit_before_meta_rename => try expectRecovered(image, 5, SNAP_A),
            .commit_after_meta_rename, .commit_after_data_rename, .persisted => try expectRecovered(image, 9, SNAP_B),
        }
    }
}

// ---------------------------------------------------------------------------
// The staged receive path elsewhere.
// ---------------------------------------------------------------------------

test "SONNY-6666: a lost chunk of a staged install resumes, and the install completes" {
    var store: Store = undefined;
    try store.init(alloc);
    defer store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 }, .stage_received_snapshots = true }, store.fs.storage());
    defer n.deinit();

    const third = SNAP_B.len / 3;
    try n.step(1, chunk(9, 0, SNAP_B[0..third], false, SNAP_B));
    try n.step(1, chunk(9, 2 * third, SNAP_B[2 * third ..], true, SNAP_B)); // middle lost
    {
        const rd = try n.ready();
        try std.testing.expect(rd.staged_snapshot == null);
        const resp = rd.messages[rd.messages.len - 1].msg.install_snapshot_response;
        try std.testing.expect(!resp.success);
        try std.testing.expectEqual(@as(u64, third), resp.next_offset);
        try n.advance();
    }
    try n.step(1, chunk(9, third, SNAP_B[third .. 2 * third], false, SNAP_B));
    try n.step(1, chunk(9, 2 * third, SNAP_B[2 * third ..], true, SNAP_B));
    const rd = try n.ready();
    try std.testing.expect(rd.staged_snapshot != null);
    try n.advance(); // syncs under the lock when the consumer did not
    const saved = (try store.fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualStrings(SNAP_B, saved.data);
}

test "SONNY-6666: a staged install with a bad digest is refused and leaves nothing behind" {
    var store: Store = undefined;
    try store.init(alloc);
    defer store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 }, .stage_received_snapshots = true }, store.fs.storage());
    defer n.deinit();

    var msg = chunk(9, 0, SNAP_B, true, SNAP_B);
    msg.install_snapshot.checksum[0] ^= 1;
    try n.step(1, msg);
    const rd = try n.ready();
    try std.testing.expect(rd.staged_snapshot == null);
    try std.testing.expect(!rd.messages[0].msg.install_snapshot_response.success);
    try n.advance();
    try std.testing.expectEqual(@as(u64, 0), n.log.snapshot_index);
    try std.testing.expectEqual(@as(usize, 0), try countFiles(store.path, "snapshot.stage."));
}

test "SONNY-6666: a storage without staging still installs through the fallback, data in the Ready" {
    var mem_store = stg.InMemoryStorage.init(alloc);
    defer mem_store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 }, .stage_received_snapshots = true }, mem_store.storage());
    defer n.deinit();

    const third = SNAP_B.len / 3;
    try n.step(1, chunk(9, 0, SNAP_B[0..third], false, SNAP_B));
    try n.step(1, chunk(9, third, SNAP_B[third..], true, SNAP_B));
    const rd = try n.ready();
    try std.testing.expectEqualStrings(SNAP_B, rd.snapshot.?.data);
    try n.advance();
    const saved = (try mem_store.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualStrings(SNAP_B, saved.data);
    try std.testing.expectEqual(@as(u64, 9), n.log.applied);
}
