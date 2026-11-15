// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Basic usage example for the raft library.
//!
//! Demonstrates the consumer-driven ready-state loop:
//!   tick / step → ready → advancePersist → send → apply → advanceApply
//!
//! A single-node cluster elects itself, a command is proposed, and the loop
//! runs until the command is committed and applied. In a real application
//! use `raft.FileStorage` for persistence, feed `node.step(from, msg)` with
//! inbound messages, and send `Ready.messages` over your network.

const std = @import("std");
const raft = @import("raft");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var store = raft.InMemoryStorage.init(allocator);
    defer store.deinit();

    var node = try raft.RaftNode.init(allocator, .{ .id = 1, .peers = &.{}, .rng_seed = 1 }, store.storage());
    defer node.deinit();

    std.debug.print("raft — basic ready-state loop example\n", .{});

    var proposed = false;
    var applied = false;
    var rounds: u32 = 0;
    while (!applied and rounds < 100) : (rounds += 1) {
        try node.tick(null);
        if (node.role == .leader and !proposed) {
            try node.propose("set x = 1");
            proposed = true;
        }

        const rd = try node.ready();
        // 1. Persist the hard state, new entries and any snapshot.
        try node.advancePersist();
        // 2. Only now send the messages: they announce what was persisted.
        for (rd.messages) |out| {
            std.debug.print("  send {s} to {d}\n", .{ @tagName(out.msg), out.to });
        }
        // 3. Apply committed entries to the state machine.
        for (rd.committed_entries) |entry| {
            if (entry.kind != .normal) continue;
            std.debug.print("  applied #{d}: {s}\n", .{ entry.index, entry.data });
            applied = true;
        }
        // 4. Release the Ready.
        try node.advanceApply();
    }

    if (!applied) return error.NotCommitted;
    std.debug.print("committed and applied after {d} rounds\n", .{rounds});
}
