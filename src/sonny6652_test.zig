// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6652 regressions, driven through a real three-node cluster on
//! FileStorage under the leak-checking allocator.
//!
//! 1. A follower that already held a re-sent snapshot answered
//!    StaleSnapshot (success=false): the leader counted a refusal, kept the
//!    send in flight and backed off, and never advanced the follower.
//! 2. A follower restarted on its snapshot re-installed (re-wrote) a re-sent
//!    copy of the same snapshot.
//! 3. One InstallSnapshot chunk lost in transit made the follower refuse
//!    every chunk after it: each refusal doubled the leader's back-off, and
//!    the next send started over from offset 0.

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");
const node_mod = @import("node.zig");
const fstg = @import("file_storage.zig");
const tc = @import("cluster_testing.zig");

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

    fn deinit(self: *Store) void {
        self.fs.deinit();
        alloc.free(self.path);
        self.tmp.cleanup();
    }
};

const Fixture = struct {
    router: tc.Router,
    cluster: tc.Cluster,
    stores: [tc.MAX_NODES + 1]Store,
    nodes: [tc.MAX_NODES + 1]RaftNode,
    chunk_size: usize,

    fn config(self: *const Fixture, id: u64) node_mod.Config {
        const peers: []const u64 = switch (id) {
            1 => &.{ 2, 3 },
            2 => &.{ 1, 3 },
            else => &.{ 1, 2 },
        };
        return .{ .id = id, .peers = peers, .pre_vote = false, .rng_seed = id, .snapshot_chunk_size = self.chunk_size };
    }

    fn init(self: *Fixture, chunk_size: usize) !void {
        self.chunk_size = chunk_size;
        self.router.init(alloc);
        self.cluster = .{ .router = &self.router };
        var id: u64 = 1;
        while (id <= tc.MAX_NODES) : (id += 1) {
            try self.stores[id].init();
            try self.start(id);
        }
    }

    fn start(self: *Fixture, id: u64) !void {
        self.nodes[id] = try RaftNode.init(alloc, self.config(id), self.stores[id].fs.storage());
        self.nodes[id].setTransport(self.router.transport(id));
        self.cluster.nodes[id] = &self.nodes[id];
    }

    /// Crash and restart node `id` on its on-disk state.
    fn restart(self: *Fixture, id: u64) !void {
        self.nodes[id].deinit();
        self.cluster.nodes[id] = null;
        self.stores[id].fs.deinit();
        self.stores[id].fs = try FileStorage.init(alloc, self.stores[id].path);
        try self.start(id);
    }

    fn deinit(self: *Fixture) void {
        var id: u64 = 1;
        while (id <= tc.MAX_NODES) : (id += 1) {
            if (self.cluster.nodes[id] != null) self.nodes[id].deinit();
            self.stores[id].deinit();
        }
        self.router.deinit();
    }

    /// Node 1 leads; node 2 misses entries the leader then snapshots and
    /// compacts away, so node 2 can only catch up by InstallSnapshot.
    /// Returns the snapshot index.
    fn leaderWithLaggard(self: *Fixture, data: []const u8) !u64 {
        try self.cluster.electOne();
        self.router.isolated[2] = true;
        try self.cluster.proposeMany(5);
        const leader = self.cluster.node(1);
        try std.testing.expect(leader.log.applied >= 6);
        try leader.reportSnapshot(data);
        try self.cluster.settle();
        self.router.isolated[2] = false;
        return leader.log.snapshot_index;
    }
};

/// Drop the first InstallSnapshotResponse node 2 sends.
const DropFirstAck = struct {
    dropped: u32 = 0,

    fn f(ctx: *anyopaque, env: tc.Envelope) bool {
        const self: *DropFirstAck = @ptrCast(@alignCast(ctx));
        if (env.from == 2 and env.msg == .install_snapshot_response and self.dropped == 0) {
            self.dropped += 1;
            return true;
        }
        return false;
    }
};

/// Tick the leader for up to `rounds` heartbeat rounds, stopping once node 2
/// is no longer waiting on a snapshot.
fn tickUntilAcked(fx: *Fixture, rounds: u32) !void {
    var r: u32 = 0;
    while (r < rounds) : (r += 1) {
        try fx.cluster.tick(1);
        if (fx.cluster.progress(2).pending_snapshot_index == 0) return;
    }
}

test "SONNY-6652: a follower acks a re-sent snapshot it already installed, and the leader advances" {
    var fx: Fixture = undefined;
    try fx.init(64 * 1024);
    defer fx.deinit();

    const snap_index = try fx.leaderWithLaggard("state-machine-at-snapshot");

    // The follower installs the snapshot but its ack is lost.
    var drop = DropFirstAck{};
    fx.router.drop = &DropFirstAck.f;
    fx.router.drop_ctx = @ptrCast(&drop);
    try fx.cluster.tick(1);
    try std.testing.expectEqual(@as(u32, 1), drop.dropped);
    try std.testing.expectEqual(@as(u64, 1), fx.cluster.node(2).stats().snapshots_installed);
    try std.testing.expectEqual(snap_index, fx.cluster.node(2).log.snapshot_index);
    try std.testing.expectEqual(snap_index, fx.cluster.progress(2).pending_snapshot_index);

    // The leader re-sends it after the retry interval. The follower already
    // holds it: that must count as an install, not a refusal.
    try tickUntilAcked(&fx, node_mod.SNAPSHOT_RESEND_TICKS + 2);
    const progress = fx.cluster.progress(2);
    try std.testing.expectEqual(@as(u32, 0), progress.snapshot_refusals);
    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot_index);
    try std.testing.expect(progress.match_index >= snap_index);
    // Installed once: the re-send was acknowledged, not installed again.
    try std.testing.expectEqual(@as(u64, 1), fx.cluster.node(2).stats().snapshots_installed);

    // Replication resumes by AppendEntries.
    try fx.cluster.proposeMany(2);
    try std.testing.expectEqual(fx.cluster.node(1).log.lastIndex(), fx.cluster.node(2).log.lastIndex());
    try std.testing.expectEqual(fx.cluster.node(1).log.committed, fx.cluster.node(2).log.committed);
}

test "SONNY-6652: a follower restarted on its snapshot acks a re-send instead of reinstalling it" {
    var fx: Fixture = undefined;
    try fx.init(64 * 1024);
    defer fx.deinit();

    const snap_index = try fx.leaderWithLaggard("state-machine-at-snapshot");

    var drop = DropFirstAck{};
    fx.router.drop = &DropFirstAck.f;
    fx.router.drop_ctx = @ptrCast(&drop);
    try fx.cluster.tick(1);
    try std.testing.expectEqual(@as(u32, 1), drop.dropped);

    // The follower restarts on the snapshot it persisted.
    try fx.restart(2);
    try std.testing.expectEqual(snap_index, fx.cluster.node(2).log.snapshot_index);

    try tickUntilAcked(&fx, node_mod.SNAPSHOT_RESEND_TICKS + 2);
    const progress = fx.cluster.progress(2);
    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot_index);
    try std.testing.expectEqual(@as(u32, 0), progress.snapshot_refusals);
    try std.testing.expect(progress.match_index >= snap_index);
    // It already held this snapshot: it is not written again.
    try std.testing.expectEqual(@as(u64, 0), fx.cluster.node(2).stats().snapshots_installed);

    try fx.cluster.proposeMany(2);
    try std.testing.expectEqual(fx.cluster.node(1).log.lastIndex(), fx.cluster.node(2).log.lastIndex());
}

/// Drop the first chunk sent to node 2 at `offset`.
const DropChunk = struct {
    offset: u64,
    dropped: u32 = 0,

    fn f(ctx: *anyopaque, env: tc.Envelope) bool {
        const self: *DropChunk = @ptrCast(@alignCast(ctx));
        if (env.to == 2 and env.msg == .install_snapshot and
            env.msg.install_snapshot.offset == self.offset and self.dropped == 0)
        {
            self.dropped += 1;
            return true;
        }
        return false;
    }
};

test "SONNY-6652: a lost chunk resumes from the follower's offset and is not a refusal" {
    var fx: Fixture = undefined;
    try fx.init(1024);
    defer fx.deinit();

    // 11 chunks of 1 KiB (the last one short).
    var data: [11 * 1024 - 100]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i *% 31 +% 7);
    const snap_index = try fx.leaderWithLaggard(&data);

    var drop = DropChunk{ .offset = 3 * 1024 };
    fx.router.drop = &DropChunk.f;
    fx.router.drop_ctx = @ptrCast(&drop);

    const first_send = fx.router.chunks.items.len;
    try fx.cluster.tick(1);
    var buf: [64]tc.SentChunk = undefined;
    try std.testing.expectEqual(@as(usize, 11), fx.router.chunksTo(2, first_send, &buf).len);
    try std.testing.expectEqual(@as(u32, 1), drop.dropped);

    // The follower could not place the chunks after the gap. That is a lost
    // message, not a refusal of the snapshot: no back-off.
    const progress = fx.cluster.progress(2);
    try std.testing.expectEqual(@as(u32, 0), progress.snapshot_refusals);
    try std.testing.expectEqual(snap_index, progress.pending_snapshot_index);

    // The re-send starts where the follower stopped, not at 0.
    const resend = fx.router.chunks.items.len;
    try tickUntilAcked(&fx, node_mod.SNAPSHOT_RESEND_TICKS + 2);
    const sent = fx.router.chunksTo(2, resend, &buf);
    try std.testing.expectEqual(@as(usize, 8), sent.len);
    try std.testing.expectEqual(@as(u64, 3 * 1024), sent[0].offset);
    try std.testing.expect(sent[sent.len - 1].done);

    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot_index);
    try std.testing.expect(progress.match_index >= snap_index);
    try std.testing.expectEqual(@as(u64, 1), fx.cluster.node(2).stats().snapshots_installed);
    const saved = (try fx.stores[2].fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualSlices(u8, &data, saved.data);
}

fn installChunk(index: u64, term: u64, offset: u64, data: []const u8, done: bool, whole: []const u8) rpc.Message {
    var digest: [32]u8 = .{0} ** 32;
    if (done) std.crypto.hash.Blake3.hash(whole, &digest, .{});
    return .{ .install_snapshot = .{
        .term = term,
        .leader_id = 1,
        .last_included_index = index,
        .last_included_term = term,
        .offset = offset,
        .data = data,
        .done = done,
        .checksum = digest,
        .config = .{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false },
    } };
}

/// The InstallSnapshotResponse the follower queued in its next Ready.
fn takeResponse(n: *RaftNode) !rpc.InstallSnapshotResponse {
    const rd = try n.ready();
    try n.advancePersist();
    var found: ?rpc.InstallSnapshotResponse = null;
    for (rd.messages) |out| if (out.msg == .install_snapshot_response) {
        found = out.msg.install_snapshot_response;
    };
    try n.advanceApply();
    return found orelse error.NoResponse;
}

test "SONNY-6652: a follower holding a newer snapshot acks an older one with the index it holds" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer n.deinit();

    try n.step(1, installChunk(10, 2, 0, "ten", true, "ten"));
    const installed = try takeResponse(&n);
    try std.testing.expect(installed.success);
    try std.testing.expectEqual(@as(u64, 10), installed.last_index);

    try n.step(1, installChunk(8, 2, 0, "eight", true, "eight"));
    const acked = try takeResponse(&n);
    try std.testing.expect(acked.success);
    try std.testing.expectEqual(@as(u64, 10), acked.last_index);
    try std.testing.expectEqual(@as(u64, 1), n.stats().snapshots_installed);
}

test "SONNY-6652: a gap in the chunk stream reports where to resume; a foreign chunk does not splice in" {
    var store: Store = undefined;
    try store.init();
    defer store.deinit();
    var n = try RaftNode.init(alloc, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer n.deinit();

    const whole = "0123456789";
    try n.step(1, installChunk(7, 2, 0, whole[0..4], false, whole));
    try n.step(1, installChunk(7, 2, 8, whole[8..], true, whole)); // 4..8 lost
    const gap = try takeResponse(&n);
    try std.testing.expect(!gap.success);
    try std.testing.expectEqual(@as(u64, 7), gap.last_index);
    try std.testing.expectEqual(@as(u64, 4), gap.next_offset);

    // A chunk of another snapshot at the expected offset is refused.
    try n.step(1, installChunk(9, 2, 4, whole[4..8], false, whole));
    const foreign = try takeResponse(&n);
    try std.testing.expect(!foreign.success);
    try std.testing.expectEqual(@as(u64, 9), foreign.last_index);
    try std.testing.expectEqual(@as(u64, 0), foreign.next_offset);

    // Resuming at 4 completes the snapshot.
    try n.step(1, installChunk(7, 2, 4, whole[4..8], false, whole));
    try n.step(1, installChunk(7, 2, 8, whole[8..], true, whole));
    const done = try takeResponse(&n);
    try std.testing.expect(done.success);
    try std.testing.expectEqual(@as(u64, 7), done.last_index);
    const saved = (try store.fs.storage().loadSnapshot()).?;
    defer stg.freeSnapshot(alloc, saved);
    try std.testing.expectEqualStrings(whole, saved.data);
}
