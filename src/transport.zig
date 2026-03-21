// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");

pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (*anyopaque, to: u64, msg: rpc.Message) void,
    };

    /// Send a message to a peer. Silently drops on failure (Raft retries via heartbeat).
    pub fn send(self: Transport, to: u64, msg: rpc.Message) void {
        self.vtable.send(self.ptr, to, msg);
    }
};

pub const InProcessTransport = struct {
    /// Maps node_id -> inbox (ArrayList of Messages)
    inboxes: std.AutoHashMap(u64, std.ArrayList(rpc.Message)),
    allocator: std.mem.Allocator,

    const vtable = Transport.VTable{
        .send = &sendImpl,
    };

    pub fn init(allocator: std.mem.Allocator) InProcessTransport {
        return .{
            .inboxes = std.AutoHashMap(u64, std.ArrayList(rpc.Message)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *InProcessTransport) void {
        var it = self.inboxes.valueIterator();
        while (it.next()) |inbox| {
            inbox.deinit(self.allocator);
        }
        self.inboxes.deinit();
    }

    /// Register a node so it can receive messages.
    pub fn register(self: *InProcessTransport, id: u64) !void {
        try self.inboxes.put(id, .{});
    }

    /// Get a Transport handle. The returned Transport's send() delivers messages
    /// to the target node's inbox. Multiple calls share the same underlying state.
    pub fn transport(self: *InProcessTransport) Transport {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    /// Drain all pending messages for a node. Returns owned slice, caller must free.
    pub fn drain(self: *InProcessTransport, id: u64, allocator: std.mem.Allocator) ![]rpc.Message {
        const inbox = self.inboxes.getPtr(id) orelse return try allocator.alloc(rpc.Message, 0);
        const items = inbox.items;
        const result = try allocator.alloc(rpc.Message, items.len);
        @memcpy(result, items);
        inbox.clearRetainingCapacity();
        return result;
    }

    /// Check how many messages are pending for a node.
    pub fn pendingCount(self: *InProcessTransport, id: u64) usize {
        const inbox = self.inboxes.getPtr(id) orelse return 0;
        return inbox.items.len;
    }

    fn sendImpl(ptr: *anyopaque, to: u64, msg: rpc.Message) void {
        const self: *InProcessTransport = @ptrCast(@alignCast(ptr));
        const inbox = self.inboxes.getPtr(to) orelse return;
        inbox.append(self.allocator, msg) catch return;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "send and drain single message" {
    var ipt = InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();

    try ipt.register(1);
    const t = ipt.transport();

    const msg = rpc.Message{ .request_vote = .{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } };
    t.send(1, msg);

    const msgs = try ipt.drain(1, std.testing.allocator);
    defer std.testing.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 1), msgs.len);
    try std.testing.expectEqual(@as(u64, 1), msgs[0].request_vote.term);
}

test "send to unregistered node is silently dropped" {
    var ipt = InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();

    const t = ipt.transport();
    const msg = rpc.Message{ .request_vote = .{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } };
    // Should not crash
    t.send(99, msg);
}

test "multiple messages accumulate in inbox" {
    var ipt = InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();

    try ipt.register(1);
    const t = ipt.transport();

    for (0..5) |i| {
        t.send(1, rpc.Message{ .append_entries_response = .{
            .term = @intCast(i),
            .success = true,
            .match_index = @intCast(i),
            .conflict_index = 0,
            .conflict_term = 0,
        } });
    }

    try std.testing.expectEqual(@as(usize, 5), ipt.pendingCount(1));

    const msgs = try ipt.drain(1, std.testing.allocator);
    defer std.testing.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 5), msgs.len);
    for (0..5) |i| {
        try std.testing.expectEqual(@as(u64, @intCast(i)), msgs[i].append_entries_response.term);
    }
}

test "drain clears inbox — second drain returns empty" {
    var ipt = InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();

    try ipt.register(1);
    const t = ipt.transport();

    t.send(1, rpc.Message{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
        .pre_vote = false,
    } });

    const first = try ipt.drain(1, std.testing.allocator);
    defer std.testing.allocator.free(first);
    try std.testing.expectEqual(@as(usize, 1), first.len);

    const second = try ipt.drain(1, std.testing.allocator);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqual(@as(usize, 0), second.len);
}

test "pendingCount accuracy" {
    var ipt = InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();

    // Unregistered node returns 0
    try std.testing.expectEqual(@as(usize, 0), ipt.pendingCount(42));

    try ipt.register(1);
    try std.testing.expectEqual(@as(usize, 0), ipt.pendingCount(1));

    const t = ipt.transport();
    t.send(1, rpc.Message{ .request_vote = .{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } });
    try std.testing.expectEqual(@as(usize, 1), ipt.pendingCount(1));

    t.send(1, rpc.Message{ .request_vote = .{
        .term = 2,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } });
    try std.testing.expectEqual(@as(usize, 2), ipt.pendingCount(1));

    const msgs = try ipt.drain(1, std.testing.allocator);
    defer std.testing.allocator.free(msgs);
    try std.testing.expectEqual(@as(usize, 0), ipt.pendingCount(1));
}
