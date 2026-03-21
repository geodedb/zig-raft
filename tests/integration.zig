// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Integration tests for raft
//!
//! These tests verify end-to-end functionality and cross-module interactions
//! using the test harness Network.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");

// Pull in scenario test files so they are discovered by the test runner.
comptime {
    _ = @import("scenarios/replication.zig");
    _ = @import("scenarios/membership.zig");
}

// ============================================================================
// Integration Tests
// ============================================================================

test "integration: three-node cluster elects a leader" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader = try net.waitForLeader(50);
    try std.testing.expect(leader >= 1 and leader <= 3);
}
