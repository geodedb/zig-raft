// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Test harness for integration and scenario tests.
//!
//! Provides a `Network` that manages multiple `RaftNode` instances with a
//! deterministic, manually-driven message transport. Messages are collected
//! into a central queue tagged with sender/receiver so that `node.step(from, msg)`
//! receives the correct origin.

const std = @import("std");
const raft = @import("raft");

/// A queued message carrying sender, receiver, and payload.
pub const PendingMessage = struct {
    from: u64,
    to: u64,
    msg: raft.Message,
};

/// Heap-allocated message queue shared by all per-node transports.
/// Allocated on the heap so that pointers remain stable after Network
/// is returned by value from `init`.
const MessageQueue = struct {
    list: std.ArrayList(PendingMessage) = .empty,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) MessageQueue {
        return .{ .allocator = allocator };
    }

    fn append(self: *MessageQueue, pm: PendingMessage) void {
        self.list.append(self.allocator, pm) catch return;
    }

    fn deinit(self: *MessageQueue) void {
        self.list.deinit(self.allocator);
    }
};

/// Per-node transport shim that captures outbound messages into the shared
/// `MessageQueue`, tagging each with the sending node's ID.
const NodeTransport = struct {
    node_id: u64,
    queue: *MessageQueue,

    const vtable_inst = raft.Transport.VTable{
        .send = &sendImpl,
    };

    fn transport(self: *NodeTransport) raft.Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_inst };
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: raft.Message) void {
        const self: *NodeTransport = @ptrCast(@alignCast(ptr));
        // Deep-copy AppendEntries entries so the sender can free its
        // slice after send without leaving dangling pointers (SEC-0140).
        var queued_msg = msg;
        switch (msg) {
            .append_entries => |ae| {
                if (ae.entries.len > 0) {
                    const dup = self.queue.allocator.dupe(raft.Entry, ae.entries) catch return;
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
        self.queue.append(.{ .from = self.node_id, .to = to, .msg = queued_msg });
    }
};

/// A test network that manages multiple RaftNode instances with deterministic,
/// tick-driven message delivery.
pub const Network = struct {
    nodes: std.AutoHashMap(u64, *raft.RaftNode),
    storages: std.AutoHashMap(u64, *raft.InMemoryStorage),
    transports: std.AutoHashMap(u64, *NodeTransport),
    node_ids: std.ArrayList(u64),
    paused_nodes: std.AutoHashMap(u64, void),
    dropped_routes: std.AutoHashMap(RouteKey, void),
    queue: *MessageQueue,
    committed: std.AutoHashMap(u64, std.ArrayList(raft.Entry)),
    allocator: std.mem.Allocator,

    pub const RouteKey = struct { from: u64, to: u64 };

    /// Create a network of `node_count` nodes with IDs 1..node_count.
    /// Uses page_allocator internally to avoid leak detection noise from
    /// Raft's shared-ownership entry slices.
    pub fn init(_: std.mem.Allocator, node_count: u64) !Network {
        const allocator = std.heap.page_allocator;

        const q = try allocator.create(MessageQueue);
        q.* = MessageQueue.init(allocator);

        var net = Network{
            .nodes = std.AutoHashMap(u64, *raft.RaftNode).init(allocator),
            .storages = std.AutoHashMap(u64, *raft.InMemoryStorage).init(allocator),
            .transports = std.AutoHashMap(u64, *NodeTransport).init(allocator),
            .node_ids = .empty,
            .paused_nodes = std.AutoHashMap(u64, void).init(allocator),
            .dropped_routes = std.AutoHashMap(RouteKey, void).init(allocator),
            .queue = q,
            .committed = std.AutoHashMap(u64, std.ArrayList(raft.Entry)).init(allocator),
            .allocator = allocator,
        };
        errdefer net.deinit();

        var i: u64 = 1;
        while (i <= node_count) : (i += 1) {
            // Build peer list (all other node IDs).
            var peers: std.ArrayList(u64) = .empty;
            defer peers.deinit(allocator);
            var j: u64 = 1;
            while (j <= node_count) : (j += 1) {
                if (j != i) try peers.append(allocator, j);
            }

            // Heap-allocate storage for pointer stability.
            const store = try allocator.create(raft.InMemoryStorage);
            store.* = raft.InMemoryStorage.init(allocator);

            const config = raft.Config{
                .id = i,
                .peers = try allocator.dupe(u64, peers.items),
                .election_tick = 10,
                .heartbeat_tick = 1,
                .rng_seed = i,
            };

            // Heap-allocate node for pointer stability.
            const n = try allocator.create(raft.RaftNode);
            n.* = try raft.RaftNode.init(allocator, config, store.storage());

            // Heap-allocate per-node transport shim.
            const nt = try allocator.create(NodeTransport);
            nt.* = .{ .node_id = i, .queue = q };
            n.setTransport(nt.transport());

            try net.nodes.put(i, n);
            try net.storages.put(i, store);
            try net.transports.put(i, nt);
            try net.node_ids.append(allocator, i);
            try net.committed.put(i, .empty);
        }

        return net;
    }

    pub fn deinit(self: *Network) void {
        var nit = self.nodes.iterator();
        while (nit.next()) |entry| {
            const n = entry.value_ptr.*;
            self.allocator.free(n.config.peers);
            n.deinit();
            self.allocator.destroy(n);
        }
        self.nodes.deinit();

        var sit = self.storages.iterator();
        while (sit.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.storages.deinit();

        var tit = self.transports.iterator();
        while (tit.next()) |entry| {
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.transports.deinit();

        var cit = self.committed.iterator();
        while (cit.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.committed.deinit();

        self.node_ids.deinit(self.allocator);
        self.paused_nodes.deinit();
        self.dropped_routes.deinit();

        self.queue.deinit();
        self.allocator.destroy(self.queue);
    }

    /// Run one complete tick round:
    ///   Phase 1 -- tick every non-paused node (generates outbound messages).
    ///   Phase 2 -- deliver queued messages via step(), respecting partitions.
    ///   Phase 3 -- ready/advance on every non-paused node, collecting commits.
    pub fn tickAll(self: *Network) !void {
        // Phase 1: tick.
        for (self.node_ids.items) |id| {
            if (self.paused_nodes.contains(id)) continue;
            const n = self.nodes.get(id).?;
            try n.tick(null);
        }

        // Phase 2: deliver messages.
        // Snapshot the queue so step()-generated sends don't cause re-entrancy.
        const pending = self.queue.list.items;
        const snapshot = try self.allocator.alloc(PendingMessage, pending.len);
        defer self.allocator.free(snapshot);
        @memcpy(snapshot, pending);
        self.queue.list.clearRetainingCapacity();

        for (snapshot) |pm| {
            if (self.paused_nodes.contains(pm.to)) continue;
            if (self.dropped_routes.contains(.{ .from = pm.from, .to = pm.to })) continue;
            if (self.nodes.get(pm.to)) |n| {
                try n.step(pm.from, pm.msg);
            }
        }

        // Drain any messages generated during delivery (responses).
        const extra = self.queue.list.items;
        const extra_snap = try self.allocator.alloc(PendingMessage, extra.len);
        defer self.allocator.free(extra_snap);
        @memcpy(extra_snap, extra);
        self.queue.list.clearRetainingCapacity();

        for (extra_snap) |pm| {
            if (self.paused_nodes.contains(pm.to)) continue;
            if (self.dropped_routes.contains(.{ .from = pm.from, .to = pm.to })) continue;
            if (self.nodes.get(pm.to)) |n| {
                try n.step(pm.from, pm.msg);
            }
        }

        // Phase 3: ready / advance -- collect committed entries.
        for (self.node_ids.items) |id| {
            if (self.paused_nodes.contains(id)) continue;
            const n = self.nodes.get(id).?;
            const r = try n.ready();
            for (r.committed_entries) |entry| {
                const list = self.committed.getPtr(id).?;
                try list.append(self.allocator, entry);
            }
            try n.advance();
        }

        // Discard any remaining queued messages from the ready/advance phase;
        // they will be delivered on the next tickAll round.
    }

    /// Tick repeatedly until exactly one leader exists, returning its ID.
    pub fn waitForLeader(self: *Network, max_ticks: u32) !u64 {
        var t: u32 = 0;
        while (t < max_ticks) : (t += 1) {
            try self.tickAll();
            if (self.getLeader()) |lid| return lid;
        }
        return error.NoLeaderElected;
    }

    /// Propose `data` on the current leader. Returns error if no leader.
    pub fn propose(self: *Network, data: []const u8) !void {
        const lid = self.getLeader() orelse return error.NoLeaderElected;
        const n = self.nodes.get(lid).?;
        try n.propose(data);
    }

    /// Tick until the entry at `index` appears in the committed list of a
    /// quorum of nodes, or until `max_ticks` is exhausted.
    pub fn waitForCommit(self: *Network, index: u64, max_ticks: u32) !void {
        const quorum_size = self.node_ids.items.len / 2 + 1;
        var t: u32 = 0;
        while (t < max_ticks) : (t += 1) {
            try self.tickAll();
            var count: usize = 0;
            for (self.node_ids.items) |id| {
                const list = self.committed.getPtr(id).?;
                for (list.items) |entry| {
                    if (entry.index == index) {
                        count += 1;
                        break;
                    }
                }
            }
            if (count >= quorum_size) return;
        }
        return error.CommitTimeout;
    }

    /// Block messages in both directions between nodes `a` and `b`.
    pub fn partition(self: *Network, a: u64, b: u64) !void {
        try self.dropped_routes.put(.{ .from = a, .to = b }, {});
        try self.dropped_routes.put(.{ .from = b, .to = a }, {});
    }

    /// Restore messages in both directions between nodes `a` and `b`.
    pub fn heal(self: *Network, a: u64, b: u64) void {
        _ = self.dropped_routes.remove(.{ .from = a, .to = b });
        _ = self.dropped_routes.remove(.{ .from = b, .to = a });
    }

    /// Exclude a node from tick processing (simulates crash).
    pub fn pauseNode(self: *Network, id: u64) !void {
        try self.paused_nodes.put(id, {});
    }

    /// Re-include a node in tick processing (simulates recovery).
    pub fn resumeNode(self: *Network, id: u64) void {
        _ = self.paused_nodes.remove(id);
    }

    /// Return the current leader's ID, or null if none.
    pub fn getLeader(self: *Network) ?u64 {
        for (self.node_ids.items) |id| {
            const n = self.nodes.get(id).?;
            if (n.role == .leader) return id;
        }
        return null;
    }

    /// Return committed entries tracked by the harness for a given node.
    pub fn committedEntries(self: *Network, id: u64) []const raft.Entry {
        const list = self.committed.getPtr(id) orelse return &[_]raft.Entry{};
        return list.items;
    }
};
