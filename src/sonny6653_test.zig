// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6653 regressions: snapshot I/O under the consumer's node lock.
//!
//! `FileStorage.saveSnapshotImpl` wrote and fsynced the whole snapshot, and
//! the leader's `sendSnapshotTo` read it back whole, both inside calls a
//! consumer makes under its node lock (`reportSnapshot`, `tick`). These
//! tests drive the staged API (stage off the lock, commit under it) and the
//! streamed leader send (the Ready hands the send to the consumer, which
//! reads the snapshot off the lock) through real RaftNodes on FileStorage,
//! time the part that runs under the lock, and crash the commit at each
//! step to show the previous snapshot survives until the commit point.

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");
const fstg = @import("file_storage.zig");
const snap_mod = @import("snapshot.zig");
const tc = @import("cluster_testing.zig");

const RaftNode = node_mod.RaftNode;
const FileStorage = fstg.FileStorage;
const alloc = std.testing.allocator;

/// The lock budget a commit or a streaming leader's tick must stay under,
/// whatever the snapshot size. Geode's whole tick budget is 250 ms; a
/// 64 MiB write + fsync blew through it.
const LOCK_BUDGET_NS: u64 = 100 * std.time.ns_per_ms;

const BIG: usize = 64 * 1024 * 1024;

fn nowNs() u64 {
    const io = std.Io.Threaded.global_single_threaded.io();
    return @intCast(std.Io.Clock.Timestamp.now(io, .awake).raw.nanoseconds);
}

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

    /// Names in the storage directory that start with `prefix`.
    fn countFiles(self: *Store, prefix: []const u8) !usize {
        const io = std.Io.Threaded.global_single_threaded.io();
        var dir = try std.Io.Dir.cwd().openDir(io, self.path, .{ .iterate = true });
        defer dir.close(io);
        var it = dir.iterate();
        var n: usize = 0;
        while (try it.next(io)) |e| {
            if (std.mem.startsWith(u8, e.name, prefix)) n += 1;
        }
        return n;
    }
};

fn bigData() ![]u8 {
    const data = try alloc.alloc(u8, BIG);
    var x: u64 = 0x9e3779b97f4a7c15;
    for (data) |*b| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        b.* = @truncate(x);
    }
    return data;
}

fn digestOf(data: []const u8) [32]u8 {
    var d: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(data, &d, .{});
    return d;
}

/// A single-voter leader on `store` with `count` applied entries.
fn singleLeader(store: *Store, count: u32) !RaftNode {
    var n = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{}, .pre_vote = false, .rng_seed = 1 }, store.fs.storage());
    errdefer n.deinit();
    var i: u32 = 0;
    while (i < 50 and n.role != .leader) : (i += 1) {
        try n.tick(null);
        _ = try n.ready();
        try n.advance();
    }
    try std.testing.expectEqual(.leader, n.role);
    i = 0;
    while (i < count) : (i += 1) try n.propose("entry");
    _ = try n.ready();
    try n.advance();
    _ = try n.ready();
    try n.advance();
    return n;
}

test "SONNY-6653: committing a staged 64 MiB snapshot holds the node lock briefly" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try singleLeader(&store, 8);
    defer n.deinit();

    const data = try bigData();
    defer alloc.free(data);

    // Under the lock: where the snapshot is taken.
    var point = try n.snapshotPoint();
    defer point.deinit();
    try std.testing.expect(point.index >= 8);

    // Off the lock: write and fsync the 64 MiB.
    const storage = store.fs.storage();
    var staged = try storage.stageSnapshot(alloc, .{ .metadata = point.metadata(), .data = data });
    defer storage.releaseStagedSnapshot(&staged);

    // Under the lock: the commit.
    const t0 = nowNs();
    try n.commitSnapshot(&staged);
    const held = nowNs() - t0;
    std.log.info("SONNY-6653: commit of a staged 64 MiB snapshot held the lock {d} us", .{held / 1000});
    try std.testing.expect(held < LOCK_BUDGET_NS);

    try std.testing.expectEqual(point.index, n.log.snapshot_index);
    n.deinit();
    try store.reopen();
    n = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{}, .rng_seed = 1 }, store.fs.storage());
    try std.testing.expectEqual(point.index, n.log.snapshot_index);
    const saved = (try store.fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqual(BIG, saved.data.len);
    try std.testing.expect(std.mem.eql(u8, data, saved.data));
    try std.testing.expectEqual(@as(usize, 0), try store.countFiles("snapshot.stage."));
    try std.testing.expectEqual(@as(usize, 0), try store.countFiles("snapshot.commit."));
}

test "SONNY-6653: a staged snapshot the log has moved past is refused, and the newer one stays" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try singleLeader(&store, 4);
    defer n.deinit();
    const storage = store.fs.storage();

    var old_point = try n.snapshotPoint();
    defer old_point.deinit();
    var old = try storage.stageSnapshot(alloc, .{ .metadata = old_point.metadata(), .data = "older-state" });
    defer storage.releaseStagedSnapshot(&old);

    var i: u32 = 0;
    while (i < 3) : (i += 1) try n.propose("more");
    _ = try n.ready();
    try n.advance();
    _ = try n.ready();
    try n.advance();
    var new_point = try n.snapshotPoint();
    defer new_point.deinit();
    var newer = try storage.stageSnapshot(alloc, .{ .metadata = new_point.metadata(), .data = "newer-state" });
    defer storage.releaseStagedSnapshot(&newer);
    try n.commitSnapshot(&newer);

    try std.testing.expectError(error.SnapshotOutOfDate, n.commitSnapshot(&old));
    const saved = (try storage.loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualStrings("newer-state", saved.data);
    try std.testing.expectEqual(new_point.index, saved.metadata.index);
}

test "SONNY-6653: the Storage fallback stages and commits on a storage without the staged API" {
    var mem_store = stg.InMemoryStorage.init(alloc);
    defer mem_store.deinit();
    const storage = mem_store.storage();
    var staged = try storage.stageSnapshot(alloc, .{
        .metadata = .{ .index = 3, .term = 1, .config = .{ .voters = &.{ 1, 2 }, .voters_outgoing = &.{}, .is_joint = false } },
        .data = "in-memory",
    });
    defer storage.releaseStagedSnapshot(&staged);
    try storage.commitStagedSnapshot(&staged);
    const saved = (try storage.loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualStrings("in-memory", saved.data);
    try std.testing.expectEqual(@as(u64, 3), saved.metadata.index);
    try std.testing.expectError(error.SnapshotAlreadyCommitted, storage.commitStagedSnapshot(&staged));
}

// ---------------------------------------------------------------------------
// Crashes between stage and commit, and inside the commit.
// ---------------------------------------------------------------------------

const cfg3 = rpc.ClusterConfig{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false };

fn tenEntries(fs: *FileStorage) !void {
    var ents: [10]rpc.Entry = undefined;
    for (&ents, 0..) |*e, i| e.* = .{ .index = i + 1, .term = 1, .kind = .normal, .data = "entry" };
    try fs.appendEntriesImpl(&ents);
}

/// The store holds snapshot A at 5; snapshot B at 8 is staged.
fn previousAndStaged(store: *Store) !stg.StagedSnapshot {
    try tenEntries(&store.fs);
    try store.fs.storage().saveSnapshot(.{ .metadata = .{ .index = 5, .term = 1, .config = cfg3 }, .data = "snapshot-A" });
    return store.fs.storage().stageSnapshot(alloc, .{ .metadata = .{ .index = 8, .term = 1, .config = cfg3 }, .data = "snapshot-B" });
}

fn expectSnapshot(store: *Store, index: u64, data: []const u8) !void {
    try store.reopen();
    try std.testing.expectEqual(index, store.fs.snapshot_index);
    const saved = (try store.fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqual(index, saved.metadata.index);
    try std.testing.expectEqualStrings(data, saved.data);
    try std.testing.expectEqualSlices(u64, cfg3.voters, saved.metadata.config.voters);
    // Recovery removed the leftovers.
    try std.testing.expectEqual(@as(usize, 0), try store.countFiles("snapshot.stage."));
    try std.testing.expectEqual(@as(usize, 0), try store.countFiles("snapshot.commit."));
    // A node starts on it.
    var n = try RaftNode.init(alloc, .{ .id = 1, .peers = &.{ 2, 3 } }, store.fs.storage());
    defer n.deinit();
    try std.testing.expectEqual(index, n.log.snapshot_index);
    try std.testing.expectEqual(@as(u64, 10), n.log.lastIndex());
}

test "SONNY-6653: a crash between stage and commit leaves the previous snapshot intact" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var staged = try previousAndStaged(&store);
    try std.testing.expectEqual(@as(usize, 1), try store.countFiles("snapshot.stage."));
    // The process dies: the staged file stays on disk, nothing is committed.
    staged.deinit();
    try expectSnapshot(&store, 5, "snapshot-A");
}

test "SONNY-6653: a crash inside the commit before its commit point leaves the previous snapshot intact" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var staged = try previousAndStaged(&store);
    store.fs.crash_at = .before_meta_rename;
    try std.testing.expectError(error.SimulatedCrash, store.fs.commitStagedSnapshot(&staged));
    staged.deinit();
    try std.testing.expectEqual(@as(usize, 1), try store.countFiles("snapshot.commit."));
    try expectSnapshot(&store, 5, "snapshot-A");
}

test "SONNY-6653: a crash after the commit point completes the commit on recovery" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var staged = try previousAndStaged(&store);
    store.fs.crash_at = .after_meta_rename;
    try std.testing.expectError(error.SimulatedCrash, store.fs.commitStagedSnapshot(&staged));
    staged.deinit();
    try expectSnapshot(&store, 8, "snapshot-B");
}

// ---------------------------------------------------------------------------
// The leader streams a snapshot send off the lock.
// ---------------------------------------------------------------------------

const StreamFixture = struct {
    router: tc.Router,
    cluster: tc.Cluster,
    stores: [tc.MAX_NODES + 1]Store,
    nodes: [tc.MAX_NODES + 1]RaftNode,

    fn init(self: *StreamFixture) !void {
        self.router.init(alloc);
        self.cluster = .{ .router = &self.router };
        var id: u64 = 1;
        while (id <= tc.MAX_NODES) : (id += 1) {
            try self.stores[id].init();
            const peers: []const u64 = switch (id) {
                1 => &.{ 2, 3 },
                2 => &.{ 1, 3 },
                else => &.{ 1, 2 },
            };
            self.nodes[id] = try RaftNode.init(alloc, .{
                .id = id,
                .peers = peers,
                .pre_vote = false,
                .rng_seed = id,
                .stream_snapshots = true,
                .snapshot_chunk_size = 4 * 1024 * 1024,
            }, self.stores[id].fs.storage());
            self.nodes[id].setTransport(self.router.transport(id));
            self.cluster.nodes[id] = &self.nodes[id];
        }
    }

    fn deinit(self: *StreamFixture) void {
        var id: u64 = 1;
        while (id <= tc.MAX_NODES) : (id += 1) {
            self.nodes[id].deinit();
            self.stores[id].deinit();
        }
        self.cluster.deinit();
        self.router.deinit();
    }

    /// The consumer streams every queued send, off the lock.
    fn streamSends(self: *StreamFixture) !void {
        const sends = try alloc.dupe(@TypeOf(self.cluster.sends.items[0]), self.cluster.sends.items);
        defer alloc.free(sends);
        self.cluster.sends.clearRetainingCapacity();
        for (sends) |req| {
            var reader = (try self.stores[1].fs.openSnapshotReader()).?;
            defer reader.close();
            try snap_mod.streamSnapshot(alloc, &reader, req, self.router.transport(1));
        }
    }
};

test "SONNY-6653: a streaming leader's tick does no snapshot I/O; the consumer streams 64 MiB off the lock" {
    var fx: StreamFixture = undefined;
    try fx.init();
    defer fx.deinit();

    try fx.cluster.electOne();
    fx.router.isolated[2] = true;
    try fx.cluster.proposeMany(5);
    const leader = fx.cluster.node(1);

    const data = try bigData();
    defer alloc.free(data);
    var point = try leader.snapshotPoint();
    defer point.deinit();
    const storage = fx.stores[1].fs.storage();
    var staged = try storage.stageSnapshot(alloc, .{ .metadata = point.metadata(), .data = data });
    defer storage.releaseStagedSnapshot(&staged);
    try leader.commitSnapshot(&staged);
    try fx.cluster.settle();
    fx.router.isolated[2] = false;

    // Under the lock: the tick that finds node 2 behind the snapshot, and
    // the consumer cycle after it.
    const t0 = nowNs();
    try leader.tick(null);
    try fx.cluster.flush(leader);
    const held = nowNs() - t0;
    std.log.info("SONNY-6653: streaming leader tick held the lock {d} us", .{held / 1000});
    try std.testing.expect(held < LOCK_BUDGET_NS);
    try std.testing.expectEqual(@as(usize, 1), fx.cluster.sends.items.len);
    const req = fx.cluster.sends.items[0];
    try std.testing.expectEqual(@as(u64, 2), req.to);
    try std.testing.expectEqual(point.index, req.index);
    try std.testing.expectEqual(point.index, fx.cluster.progress(2).pending_snapshot_index);
    // Nothing was read or sent by the node itself.
    for (fx.router.chunks.items) |c| try std.testing.expect(c.to != 2);

    try fx.streamSends();
    try fx.cluster.settle();

    try std.testing.expectEqual(@as(u64, 0), fx.cluster.progress(2).pending_snapshot_index);
    try std.testing.expect(fx.cluster.progress(2).match_index >= point.index);
    const saved = (try fx.stores[2].fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expect(std.mem.eql(u8, data, saved.data));

    try fx.cluster.proposeMany(2);
    try std.testing.expectEqual(leader.log.lastIndex(), fx.cluster.node(2).log.lastIndex());
}

test "SONNY-6653: a streamed send resumes at the requested offset and refuses data that does not match its digest" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var data: [10 * 1024]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i *% 13);
    try store.fs.storage().saveSnapshot(.{ .metadata = .{ .index = 4, .term = 2, .config = cfg3 }, .data = &data });

    var router: tc.Router = undefined;
    router.init(alloc);
    defer router.deinit();
    {
        var reader = (try store.fs.openSnapshotReader()).?;
        defer reader.close();
        try snap_mod.streamSnapshot(alloc, &reader, .{
            .to = 2,
            .term = 2,
            .leader_id = 1,
            .index = 4,
            .offset = 3 * 1024,
            .chunk_size = 2 * 1024,
        }, router.transport(1));
    }
    // 7 KiB from 3 KiB on, in 2 KiB chunks: 3, 5, 7, 9.
    try std.testing.expectEqual(@as(usize, 4), router.chunks.items.len);
    try std.testing.expectEqual(@as(u64, 3 * 1024), router.chunks.items[0].offset);
    try std.testing.expect(router.chunks.items[3].done);
    const last = router.queue.items[3].msg.install_snapshot;
    try std.testing.expectEqualSlices(u8, &digestOf(&data), &last.checksum);

    // The data file no longer matches the digest it was committed with: the
    // last chunk (the one the follower would install on) is not sent.
    {
        const io = std.Io.Threaded.global_single_threaded.io();
        const p = try std.fmt.allocPrint(alloc, "{s}/snapshot.data", .{store.path});
        defer alloc.free(p);
        var f = try std.Io.Dir.cwd().openFile(io, p, .{ .mode = .read_write });
        defer f.close(io);
        try f.writePositionalAll(io, "X", 100);
    }
    const before = router.chunks.items.len;
    var reader = (try store.fs.openSnapshotReader()).?;
    defer reader.close();
    try std.testing.expectError(error.SnapshotDigestMismatch, snap_mod.streamSnapshot(alloc, &reader, .{
        .to = 2,
        .term = 2,
        .leader_id = 1,
        .index = 4,
        .offset = 0,
        .chunk_size = 2 * 1024,
    }, router.transport(1)));
    for (router.chunks.items[before..]) |c| try std.testing.expect(!c.done);
}
