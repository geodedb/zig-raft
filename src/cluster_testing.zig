// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! A three-node in-process cluster for the snapshot regression tests
//! (SONNY-6652, SONNY-6653): real RaftNodes on real storage, and a router
//! that keeps each message's sender and can drop chosen messages. Nothing
//! here stands in for library behaviour; it only moves messages.

const std = @import("std");
const rpc = @import("rpc.zig");
const node_mod = @import("node.zig");
const trans = @import("transport.zig");
const ready_mod = @import("ready.zig");

const RaftNode = node_mod.RaftNode;

pub const MAX_NODES = 3;

pub const Envelope = struct {
    from: u64,
    to: u64,
    msg: rpc.Message,
};

/// An InstallSnapshot chunk as it left the leader.
pub const SentChunk = struct {
    to: u64,
    index: u64,
    offset: u64,
    len: usize,
    done: bool,
};

pub const Router = struct {
    allocator: std.mem.Allocator,
    queue: std.ArrayList(Envelope) = .empty,
    endpoints: [MAX_NODES + 1]Endpoint = undefined,
    /// Messages to or from an isolated node are dropped.
    isolated: [MAX_NODES + 1]bool = .{false} ** (MAX_NODES + 1),
    /// Return true to drop the message.
    drop: ?*const fn (ctx: *anyopaque, env: Envelope) bool = null,
    drop_ctx: *anyopaque = undefined,
    /// Every InstallSnapshot chunk any node sent, dropped or not.
    chunks: std.ArrayList(SentChunk) = .empty,

    const Endpoint = struct { router: *Router, id: u64 };

    const vtable = trans.Transport.VTable{ .send = &sendImpl };

    /// Initialise in place: the transports point back at the router.
    pub fn init(self: *Router, allocator: std.mem.Allocator) void {
        self.* = .{ .allocator = allocator };
        for (&self.endpoints, 0..) |*e, i| e.* = .{ .router = self, .id = i };
    }

    pub fn deinit(self: *Router) void {
        for (self.queue.items) |env| trans.InProcessTransport.freeMessage(self.allocator, env.msg);
        self.queue.deinit(self.allocator);
        self.chunks.deinit(self.allocator);
    }

    /// The transport node `id` sends through.
    pub fn transport(self: *Router, id: u64) trans.Transport {
        return .{ .ptr = @ptrCast(&self.endpoints[id]), .vtable = &vtable };
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: rpc.Message) void {
        const ep: *Endpoint = @ptrCast(@alignCast(ptr));
        ep.router.enqueue(ep.id, to, msg);
    }

    /// Queue a copy of `msg` (the sender frees its payload once send returns).
    pub fn enqueue(self: *Router, from: u64, to: u64, msg: rpc.Message) void {
        if (msg == .install_snapshot) {
            const is = msg.install_snapshot;
            self.chunks.append(self.allocator, .{
                .to = to,
                .index = is.last_included_index,
                .offset = is.offset,
                .len = is.data.len,
                .done = is.done,
            }) catch {};
        }
        const copy = copyMessage(self.allocator, msg) catch return;
        self.queue.append(self.allocator, .{ .from = from, .to = to, .msg = copy }) catch
            trans.InProcessTransport.freeMessage(self.allocator, copy);
    }

    fn dropped(self: *Router, env: Envelope) bool {
        if (env.to > MAX_NODES or env.from > MAX_NODES) return true;
        if (self.isolated[env.to] or self.isolated[env.from]) return true;
        if (self.drop) |f| return f(self.drop_ctx, env);
        return false;
    }

    /// Chunks sent to `to` since `start` (an index into `chunks`).
    pub fn chunksTo(self: *const Router, to: u64, start: usize, out: []SentChunk) []SentChunk {
        var n: usize = 0;
        for (self.chunks.items[start..]) |c| {
            if (c.to != to) continue;
            if (n == out.len) break;
            out[n] = c;
            n += 1;
        }
        return out[0..n];
    }
};

fn copyMessage(allocator: std.mem.Allocator, msg: rpc.Message) !rpc.Message {
    switch (msg) {
        .append_entries => |ae| {
            var c = ae;
            c.entries = try rpc.dupeEntries(allocator, ae.entries);
            return .{ .append_entries = c };
        },
        .install_snapshot => |is| {
            const data = try allocator.dupe(u8, is.data);
            errdefer allocator.free(data);
            const voters = try allocator.dupe(u64, is.config.voters);
            errdefer allocator.free(voters);
            const outgoing = try allocator.dupe(u64, is.config.voters_outgoing);
            var c = is;
            c.data = data;
            c.config.voters = voters;
            c.config.voters_outgoing = outgoing;
            return .{ .install_snapshot = c };
        },
        else => return msg,
    }
}

pub const Cluster = struct {
    router: *Router,
    /// Indexed by node id (1..MAX_NODES); null while a node is down.
    nodes: [MAX_NODES + 1]?*RaftNode = .{null} ** (MAX_NODES + 1),
    /// Snapshot sends Ready handed over (`Config.stream_snapshots`), copied
    /// out before advance as a consumer must.
    sends: std.ArrayList(ready_mod.SnapshotSend) = .empty,

    pub fn deinit(self: *Cluster) void {
        self.sends.deinit(self.router.allocator);
    }

    pub fn node(self: *Cluster, id: u64) *RaftNode {
        return self.nodes[id].?;
    }

    /// Run one consumer cycle on `n`: persist, then send what the Ready
    /// queued, then apply.
    pub fn flush(self: *Cluster, n: *RaftNode) !void {
        const rd = try n.ready();
        try self.sends.appendSlice(self.router.allocator, rd.snapshot_sends);
        try n.advancePersist();
        for (rd.messages) |out| self.router.enqueue(n.id, out.to, out.msg);
        try n.advanceApply();
    }

    /// Flush every node and deliver messages until the network is quiet.
    pub fn settle(self: *Cluster) !void {
        var rounds: u32 = 0;
        while (rounds < 1000) : (rounds += 1) {
            for (self.nodes) |maybe| if (maybe) |n| try self.flush(n);
            if (self.router.queue.items.len == 0) return;
            var batch = self.router.queue;
            self.router.queue = .empty;
            defer batch.deinit(self.router.allocator);
            for (batch.items) |env| {
                defer trans.InProcessTransport.freeMessage(self.router.allocator, env.msg);
                if (self.router.dropped(env)) continue;
                const target = self.nodes[env.to] orelse continue;
                try target.step(env.from, env.msg);
            }
        }
        return error.ClusterDidNotSettle;
    }

    /// Tick node `id` once and settle.
    pub fn tick(self: *Cluster, id: u64) !void {
        try self.node(id).tick(null);
        try self.settle();
    }

    /// Elect node 1: only it ticks, so only it times out.
    pub fn electOne(self: *Cluster) !void {
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            try self.tick(1);
            if (self.node(1).role == .leader) return;
        }
        return error.NoLeader;
    }

    /// Propose `count` entries on the leader (node 1) and settle them.
    pub fn proposeMany(self: *Cluster, count: u32) !void {
        var i: u32 = 0;
        while (i < count) : (i += 1) try self.node(1).propose("entry-payload");
        try self.settle();
        // Replicate, commit, then deliver the commit index.
        try self.tick(1);
        try self.tick(1);
    }

    /// The leader's progress record for `peer`.
    pub fn progress(self: *Cluster, peer: u64) *@import("replication.zig").PeerProgress {
        return self.node(1).replication_state.?.peers.getPtr(peer).?;
    }
};
