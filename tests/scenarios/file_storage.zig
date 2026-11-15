// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Integration tests for FileStorage — demonstrates using file-backed storage
//! with a full Raft cluster. This also serves as an example for consumers who
//! want to implement custom storage backends (S3, database, etc.) against the
//! `Storage` vtable interface.
//!
//! ## How to implement a custom Storage backend
//!
//! 1. Create a struct with your storage state (e.g., `MyS3Storage`).
//! 2. Implement the 8 required methods matching `Storage.VTable` signatures:
//!    - `saveHardState(hs)` — persist term + voted_for durably.
//!    - `appendEntries(entries)` — append entries; handle truncation on conflict.
//!    - `entries(lo, hi)` — return entries in [lo, hi); the caller owns the
//!      slice and each entry's data (return copies; freed with `raft.freeEntries`).
//!    - `term(index)` — return the term for a given log index.
//!    - `lastIndex()` — return the highest log index, or snapshot_index if empty.
//!    - `saveSnapshot(snap)` — persist snapshot data + metadata.
//!    - `loadSnapshot()` — return the latest snapshot, or null.
//!    - `compact(index)` — remove entries at or below index from storage.
//! 3. Create type-erased wrappers (see `FileStorage` source for pattern).
//! 4. Expose a `storage() -> Storage` method that returns the vtable wrapper.
//! 5. Pass `my_storage.storage()` to `RaftNode.init()`.

const std = @import("std");
const raft = @import("raft");

fn rio() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

const Checker = @import("checker").Checker;

const FileStorage = raft.FileStorage;
const RaftNode = raft.RaftNode;
const Transport = raft.Transport;

// ============================================================================
// Test cluster helper — manually wires nodes with FileStorage
// ============================================================================

/// A pending message in the test cluster's message queue.
const PendingMessage = struct {
    from: u64,
    to: u64,
    msg: raft.Message,
};

/// Per-node transport shim that captures outbound messages.
const TestTransport = struct {
    node_id: u64,
    queue: *std.ArrayList(PendingMessage),
    allocator: std.mem.Allocator,

    const vtable_val = Transport.VTable{
        .send = &sendImpl,
    };

    fn transport(self: *TestTransport) Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_val };
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: raft.Message) void {
        const self: *TestTransport = @ptrCast(@alignCast(ptr));
        // Deep-copy AppendEntries entries, data included: the sender frees
        // both once send returns (SEC-0140, SONNY-6592).
        var queued_msg = msg;
        switch (msg) {
            .append_entries => |ae| {
                if (ae.entries.len > 0) {
                    const dup = raft.dupeEntries(self.allocator, ae.entries) catch return;
                    queued_msg = raft.Message{ .append_entries = .{
                        .term = ae.term,
                        .leader_id = ae.leader_id,
                        .prev_log_index = ae.prev_log_index,
                        .prev_log_term = ae.prev_log_term,
                        .entries = dup,
                        .leader_commit = ae.leader_commit,
                    } };
                }
            },
            else => {},
        }
        self.queue.append(self.allocator, .{ .from = self.node_id, .to = to, .msg = queued_msg }) catch {};
    }
};

/// A test cluster backed by FileStorage instances.
const TestCluster = struct {
    nodes: [3]*RaftNode,
    storages: [3]*FileStorage,
    transports: [3]*TestTransport,
    queue: *std.ArrayList(PendingMessage),
    allocator: std.mem.Allocator,
    base_path: []const u8,

    fn init(_: std.mem.Allocator, base_path: []const u8) !TestCluster {
        // Use page_allocator to avoid leak detection noise from Raft's
        // shared-ownership entry slices (same pattern as harness.zig).
        const allocator = std.heap.page_allocator;

        // Clean up from prior runs.
        std.Io.Dir.cwd().deleteTree(rio(), base_path) catch {};

        const queue = try allocator.create(std.ArrayList(PendingMessage));
        queue.* = .empty;

        var cluster: TestCluster = .{
            .nodes = undefined,
            .storages = undefined,
            .transports = undefined,
            .queue = queue,
            .allocator = allocator,
            .base_path = base_path,
        };
        errdefer cluster.deinit();

        for (0..3) |i| {
            const id: u64 = @intCast(i + 1);

            // Build per-node directory path.
            var path_buf: [256]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/node{d}", .{ base_path, id }) catch unreachable;

            // Create FileStorage.
            const store = try allocator.create(FileStorage);
            store.* = try FileStorage.init(allocator, path);
            cluster.storages[i] = store;

            // Build peer list.
            var peers: [2]u64 = undefined;
            var pi: usize = 0;
            for (1..4) |j| {
                if (j != id) {
                    peers[pi] = @intCast(j);
                    pi += 1;
                }
            }
            const peer_slice = try allocator.dupe(u64, &peers);

            const config = raft.Config{
                .id = id,
                .peers = peer_slice,
                .election_tick = 10,
                .heartbeat_tick = 1,
                .rng_seed = id,
            };

            const node = try allocator.create(RaftNode);
            node.* = try RaftNode.init(allocator, config, store.storage());
            cluster.nodes[i] = node;

            // Set up transport.
            const tt = try allocator.create(TestTransport);
            tt.* = .{ .node_id = id, .queue = queue, .allocator = allocator };
            node.setTransport(tt.transport());
            cluster.transports[i] = tt;
        }

        return cluster;
    }

    fn deinit(self: *TestCluster) void {
        for (0..3) |i| {
            self.allocator.free(self.nodes[i].config.peers);
            self.nodes[i].deinit();
            self.allocator.destroy(self.nodes[i]);
            self.storages[i].deinit();
            self.allocator.destroy(self.storages[i]);
            self.allocator.destroy(self.transports[i]);
        }
        self.queue.deinit(self.allocator);
        self.allocator.destroy(self.queue);
    }

    /// Run one tick round: tick all nodes, deliver messages, ready/advance.
    fn tickAll(self: *TestCluster) !void {
        // Phase 1: tick.
        for (self.nodes) |n| {
            try n.tick(null);
        }

        // Phase 2: deliver messages.
        const snapshot = try self.allocator.alloc(PendingMessage, self.queue.items.len);
        defer self.allocator.free(snapshot);
        @memcpy(snapshot, self.queue.items);
        self.queue.clearRetainingCapacity();

        for (snapshot) |pm| {
            const node = self.getNode(pm.to) orelse continue;
            try node.step(pm.from, pm.msg);
        }

        // Drain extra messages from step responses.
        const extra = try self.allocator.alloc(PendingMessage, self.queue.items.len);
        defer self.allocator.free(extra);
        @memcpy(extra, self.queue.items);
        self.queue.clearRetainingCapacity();

        for (extra) |pm| {
            const node = self.getNode(pm.to) orelse continue;
            try node.step(pm.from, pm.msg);
        }

        // Phase 3: ready / persist / send / apply. Ready.messages leave only
        // once advancePersist() has made what they announce durable.
        for (self.nodes) |n| {
            const rd = try n.ready();
            try n.advancePersist();
            for (rd.messages) |out| {
                try self.queue.append(self.allocator, .{ .from = n.id, .to = out.to, .msg = out.msg });
            }
            try n.advanceApply();
        }
    }

    fn getNode(self: *TestCluster, id: u64) ?*RaftNode {
        for (self.nodes) |n| {
            if (n.id == id) return n;
        }
        return null;
    }

    fn getLeader(self: *TestCluster) ?u64 {
        for (self.nodes) |n| {
            if (n.role == .leader) return n.id;
        }
        return null;
    }

    fn waitForLeader(self: *TestCluster, max_ticks: u32) !u64 {
        var t: u32 = 0;
        while (t < max_ticks) : (t += 1) {
            try self.tickAll();
            if (self.getLeader()) |lid| return lid;
        }
        return error.NoLeaderElected;
    }

    /// Run all Raft invariant checks against the cluster's nodes.
    fn checkInvariants(self: *TestCluster) !void {
        try Checker.checkAll(&self.nodes);
    }
};

// ============================================================================
// Integration Tests
// ============================================================================

test "file_storage integration: cluster elects leader with file-backed storage" {
    const base = "/tmp/raft-fs-integration-elect";
    var cluster = try TestCluster.init(std.testing.allocator, base);
    defer cluster.deinit();

    const leader_id = try cluster.waitForLeader(100);
    try std.testing.expect(leader_id >= 1 and leader_id <= 3);

    // Verify hard state was persisted to the leader's FileStorage.
    const leader_idx = leader_id - 1;
    const store = cluster.storages[leader_idx];
    try std.testing.expect(store.hard_state.current_term > 0);

    try cluster.checkInvariants();
}

test "file_storage integration: propose and commit entries to disk" {
    const base = "/tmp/raft-fs-integration-commit";
    var cluster = try TestCluster.init(std.testing.allocator, base);
    defer cluster.deinit();

    const leader_id = try cluster.waitForLeader(100);
    const leader = cluster.getNode(leader_id).?;

    // Propose an entry.
    try leader.propose("file-backed-entry");

    // Tick until committed (need a quorum to replicate + commit).
    var t: u32 = 0;
    while (t < 50) : (t += 1) {
        try cluster.tickAll();
    }

    // The entry should be persisted in the leader's FileStorage WAL.
    const leader_store = cluster.storages[leader_id - 1];
    const last_idx = try leader_store.lastIndexImpl();
    try std.testing.expect(last_idx >= 2); // noop + proposed entry

    // Read entries from disk and verify our data is there.
    const entries = try leader_store.entriesImpl(1, last_idx + 1);
    defer {
        for (entries) |e| {
            if (e.data.len > 0) std.heap.page_allocator.free(e.data);
        }
        std.heap.page_allocator.free(entries);
    }

    var found = false;
    for (entries) |e| {
        if (std.mem.eql(u8, e.data, "file-backed-entry")) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);

    try cluster.checkInvariants();
}

test "file_storage integration: entries replicated to followers on disk" {
    const base = "/tmp/raft-fs-integration-replicate";
    var cluster = try TestCluster.init(std.testing.allocator, base);
    defer cluster.deinit();

    const leader_id = try cluster.waitForLeader(100);
    const leader = cluster.getNode(leader_id).?;

    try leader.propose("replicated-data");

    // Tick enough for replication.
    var t: u32 = 0;
    while (t < 50) : (t += 1) {
        try cluster.tickAll();
    }

    // Check all nodes have the entry persisted.
    var nodes_with_entry: u32 = 0;
    for (cluster.storages) |store| {
        const last_idx = store.lastIndexImpl() catch continue;
        if (last_idx < 2) continue;

        const entries = store.entriesImpl(1, last_idx + 1) catch continue;
        defer {
            for (entries) |e| {
                if (e.data.len > 0) std.heap.page_allocator.free(e.data);
            }
            std.heap.page_allocator.free(entries);
        }

        for (entries) |e| {
            if (std.mem.eql(u8, e.data, "replicated-data")) {
                nodes_with_entry += 1;
                break;
            }
        }
    }

    // At least a quorum (2 out of 3) should have it.
    try std.testing.expect(nodes_with_entry >= 2);

    try cluster.checkInvariants();
}

test "file_storage integration: hard state persists across restarts" {
    const base = "/tmp/raft-fs-integration-restart";
    {
        var cluster = try TestCluster.init(std.testing.allocator, base);
        defer cluster.deinit();

        _ = try cluster.waitForLeader(100);

        // Tick extra rounds so the leader replicates the noop to followers.
        var t: u32 = 0;
        while (t < 20) : (t += 1) {
            try cluster.tickAll();
        }

        // All nodes should have persisted their hard state.
        for (cluster.storages) |store| {
            try std.testing.expect(store.hard_state.current_term > 0);
        }

        try cluster.checkInvariants();
    }

    // Re-open the storage directories and verify state survived.
    for (1..4) |id_usize| {
        var path_buf: [256]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/node{d}", .{ base, id_usize }) catch unreachable;

        var store = try FileStorage.init(std.testing.allocator, path);
        defer store.deinit();

        // Term should be non-zero (election happened).
        try std.testing.expect(store.hard_state.current_term > 0);

        // WAL should have at least the noop entry.
        const last_idx = try store.lastIndexImpl();
        try std.testing.expect(last_idx >= 1);
    }

    // Clean up.
    std.Io.Dir.cwd().deleteTree(rio(), base) catch {};
}

test "file_storage integration: snapshot save and load in cluster" {
    const base = "/tmp/raft-fs-integration-snapshot";
    var cluster = try TestCluster.init(std.testing.allocator, base);
    defer cluster.deinit();

    const leader_id = try cluster.waitForLeader(100);
    const leader = cluster.getNode(leader_id).?;

    // Propose an entry and tick until committed.
    try leader.propose("snap-entry");
    var t: u32 = 0;
    while (t < 50) : (t += 1) {
        try cluster.tickAll();
    }

    // Create a snapshot on the leader.
    try leader.reportSnapshot("test-snapshot-data");
    try leader.advance();

    // Verify snapshot is persisted.
    const store = cluster.storages[leader_id - 1];
    const loaded = try store.loadSnapshotImpl();
    try std.testing.expect(loaded != null);
    defer raft.storage.freeSnapshot(std.heap.page_allocator, loaded.?);
    try std.testing.expectEqualStrings("test-snapshot-data", loaded.?.data);

    try cluster.checkInvariants();
}

test "file_storage integration: multiple proposals commit in order" {
    const base = "/tmp/raft-fs-integration-ordering";
    var cluster = try TestCluster.init(std.testing.allocator, base);
    defer cluster.deinit();

    const leader_id = try cluster.waitForLeader(100);
    const leader = cluster.getNode(leader_id).?;

    // Propose multiple entries.
    try leader.propose("entry-1");
    try leader.propose("entry-2");
    try leader.propose("entry-3");

    // Tick until committed.
    var t: u32 = 0;
    while (t < 50) : (t += 1) {
        try cluster.tickAll();
    }

    // Read from the leader's WAL and verify ordering.
    const store = cluster.storages[leader_id - 1];
    const last_idx = try store.lastIndexImpl();
    const entries = try store.entriesImpl(1, last_idx + 1);
    defer {
        for (entries) |e| {
            if (e.data.len > 0) std.heap.page_allocator.free(e.data);
        }
        std.heap.page_allocator.free(entries);
    }

    // Find our entries and verify order.
    var found: [3]bool = .{ false, false, false };
    var prev_idx: u64 = 0;
    for (entries) |e| {
        try std.testing.expect(e.index > prev_idx); // monotonically increasing
        prev_idx = e.index;
        if (std.mem.eql(u8, e.data, "entry-1")) found[0] = true;
        if (std.mem.eql(u8, e.data, "entry-2")) found[1] = true;
        if (std.mem.eql(u8, e.data, "entry-3")) found[2] = true;
    }

    try std.testing.expect(found[0] and found[1] and found[2]);

    try cluster.checkInvariants();
}

// ============================================================================
// torn_write_recovery — CRC validation detects and truncates corrupted WAL
// ============================================================================

test "file_storage: torn_write_recovery" {
    const base = "/tmp/raft-fs-torn-write";

    // Clean up from prior runs.
    std.Io.Dir.cwd().deleteTree(rio(), base) catch {};

    const allocator = std.heap.page_allocator;

    // Phase 1: Write valid entries to FileStorage.
    {
        var store = try FileStorage.init(allocator, base);
        defer store.deinit();

        const entries = [_]raft.Entry{
            .{ .index = 1, .term = 1, .kind = .normal, .data = "entry-one" },
            .{ .index = 2, .term = 1, .kind = .normal, .data = "entry-two" },
            .{ .index = 3, .term = 2, .kind = .normal, .data = "entry-three" },
        };
        try store.appendEntriesImpl(&entries);

        // Verify all 3 entries are readable.
        const last_idx = try store.lastIndexImpl();
        try std.testing.expectEqual(@as(u64, 3), last_idx);
    }

    // Phase 2: Corrupt the WAL by appending garbage bytes (simulating a torn write).
    {
        const io = rio();
        var dir = try std.Io.Dir.cwd().openDir(io, base, .{});
        defer dir.close(io);

        var wal_file = try dir.openFile(io, "entries.wal", .{ .mode = .read_write });
        defer wal_file.close(io);

        // Append garbage at end (0.16 uses positional I/O).
        const end_pos = try wal_file.length(io);
        const garbage = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x03, 0xFF, 0xFF };
        try wal_file.writePositionalAll(io, &garbage, end_pos);
    }

    // Phase 3: Re-open the storage. recoverWal should detect the CRC mismatch
    // on the garbage bytes and truncate the WAL to the last valid record.
    {
        var store = try FileStorage.init(allocator, base);
        defer store.deinit();

        // All 3 valid entries should still be readable.
        const last_idx = try store.lastIndexImpl();
        try std.testing.expectEqual(@as(u64, 3), last_idx);

        // Read each entry and verify contents survived.
        const entries = try store.entriesImpl(1, 4);
        defer {
            for (entries) |e| {
                if (e.data.len > 0) allocator.free(e.data);
            }
            allocator.free(entries);
        }

        try std.testing.expectEqual(@as(usize, 3), entries.len);
        try std.testing.expectEqualStrings("entry-one", entries[0].data);
        try std.testing.expectEqualStrings("entry-two", entries[1].data);
        try std.testing.expectEqualStrings("entry-three", entries[2].data);
        try std.testing.expectEqual(@as(u64, 1), entries[0].term);
        try std.testing.expectEqual(@as(u64, 2), entries[2].term);

        // Verify we can still append new entries after recovery.
        const new_entry = [_]raft.Entry{
            .{ .index = 4, .term = 2, .kind = .normal, .data = "after-recovery" },
        };
        try store.appendEntriesImpl(&new_entry);

        const final_idx = try store.lastIndexImpl();
        try std.testing.expectEqual(@as(u64, 4), final_idx);
    }

    // Clean up.
    std.Io.Dir.cwd().deleteTree(rio(), base) catch {};
}
