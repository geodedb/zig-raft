// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2026 CodePros

//! Minimal example: one RaftNode driven by a single tick loop on the main
//! thread, with parallel heartbeat AppendEntries fan-out via thread-pool.
//! Demonstrates the canonical register-once / release-once lifecycle for
//! the per-submitter fast path.

const std = @import("std");
const raft = @import("raft");
const tp = raft.thread_pool;

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{ .thread_safe = true }) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Bring up a 3-node cluster in-process for demo purposes.
    //    In real deployments, each node lives in its own process with
    //    its own storage backend.
    var storage = raft.InMemoryStorage.init(allocator);
    defer storage.deinit();

    var node = try raft.RaftNode.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_tick = 10,
        .heartbeat_tick = 1,
        .rng_seed = 42,
    }, storage.storage());
    defer node.deinit();

    // 2. Create a thread-pool sized for this node's peer count.
    //    Pin .work_stealing explicitly — heartbeat AE construction is
    //    ALU-bound, chunk durations diverge by log tail length.
    var pool = try tp.ThreadPool.init(allocator, .{
        .max_threads = 4,
        .scheduler_variant = .work_stealing,
        // Reserve one slot in the per-submitter fast-path bitmap so the
        // tick-driving thread can register below. Without this, the
        // pool allocates zero submitter queues and `registerSubmitter`
        // returns `error.NoSubmitterSlots`.
        .max_submitter_queues = 1,
    });
    defer {
        _ = pool.shutdown(.drain, 5 * std.time.ns_per_s) catch {};
        pool.deinit();
    }

    // 3. Opt the main (tick-driving) thread into the per-submitter
    //    fast path. Register ONCE before the loop; release ONCE after.
    const submitter_slot = try node.registerAsSubmitter(&pool);
    defer node.releaseAsSubmitter(&pool, submitter_slot);

    // 4. Drive ticks. In a real deployment, replace this busy loop with
    //    your event-loop integration. The `&pool` argument threads the
    //    per-submitter fast path through sendHeartbeats().
    var tick_count: u32 = 0;
    while (tick_count < 100) : (tick_count += 1) {
        try node.tick(&pool);
        tp.clock.sleep(5 * std.time.ns_per_ms);
    }

    // 5. Inspect the partial-submit counter. A healthy pool with
    //    ample queue capacity should stay at 0; a rising value means
    //    your pool is saturated and you should raise queue_capacity
    //    or spread submitters.
    const stats = node.perf_stats;
    std.debug.print("heartbeats_sent={} heartbeat_partial_submits={}\n", .{
        stats.heartbeats_sent,
        stats.heartbeat_partial_submits,
    });
}
