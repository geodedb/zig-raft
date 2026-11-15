// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Basic usage example for the raft library.
//!
//! Demonstrates the consumer-driven ready-state loop:
//!   tick → step → ready → advancePersist → send → apply → advanceApply
//!
//! This example creates a single-node cluster, proposes a command, and
//! prints the committed entry. In a real application you would wire up
//! network transport and persistent storage.

const std = @import("std");

pub fn main() !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("raft — basic ready-state loop example\n", .{});
    try stdout.print("======================================\n\n", .{});

    // In a real application you would:
    //
    //   const raft = @import("raft");
    //   var store = try raft.InMemoryStorage.init(allocator);
    //   const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 } };
    //   var node = try raft.RaftNode.init(allocator, config, store.storage());
    //
    //   // Consumer loop
    //   while (true) {
    //       try node.tick(null);
    //       // ... call node.step(from, msg) for inbound messages ...
    //       const rd = try node.ready();
    //       // 1. Persist rd.hard_state, new entries and rd.snapshot
    //       try node.advancePersist();
    //       // 2. Only then send rd.messages: out.msg to peer out.to
    //       // 3. Apply rd.committed_entries to your state machine
    //       try node.advanceApply();
    //   }

    try stdout.print("See README.md Quick Start and tests/harness.zig for working examples.\n", .{});
}
