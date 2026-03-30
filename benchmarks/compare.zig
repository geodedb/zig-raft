// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Benchmark comparison tool for raft.
//!
//! Reads two JSON result files (baseline and current), compares each metric
//! at each tier, and prints a table showing delta% and pass/fail.
//!
//! Usage: zig build compare-bench -- baseline.json current.json
//! Optional: zig build compare-bench -- baseline.json current.json 25
//!           (last arg is the regression threshold percentage, default 20)
//!
//! Exit code 0 if all pass, 1 if any regression exceeds threshold.

const std = @import("std");

const print = std.debug.print;

// ---------------------------------------------------------------------------
// JSON parsing types
// ---------------------------------------------------------------------------

const TierData = struct {
    name: []const u8 = "",
    node_count: f64 = 0,
    peer_count: f64 = 0,
    propose_ns: f64 = 0,
    tick_ns: f64 = 0,
    tick_parallel_ns: f64 = 0,
    step_ae_ns: f64 = 0,
    log_append_ns: f64 = 0,
    ready_advance_ns: f64 = 0,
    heartbeat_ops_sec: f64 = 0,
    replication_ops_sec: f64 = 0,
    skipped: bool = false,
};

const BenchResult = struct {
    version: f64 = 0,
    tiers: []TierData = &[_]TierData{},
};

// ---------------------------------------------------------------------------
// Metric definitions
// ---------------------------------------------------------------------------

const MetricKind = enum {
    /// Higher is better (throughput metrics). A decrease is a regression.
    higher_is_better,
    /// Lower is better (latency metrics). An increase is a regression.
    lower_is_better,
};

const Metric = struct {
    name: []const u8,
    kind: MetricKind,
    get: *const fn (TierData) f64,
};

fn getPropose(t: TierData) f64 {
    return t.propose_ns;
}
fn getTick(t: TierData) f64 {
    return t.tick_ns;
}
fn getTickParallel(t: TierData) f64 {
    return t.tick_parallel_ns;
}
fn getStepAE(t: TierData) f64 {
    return t.step_ae_ns;
}
fn getLogAppend(t: TierData) f64 {
    return t.log_append_ns;
}
fn getReadyAdvance(t: TierData) f64 {
    return t.ready_advance_ns;
}
fn getHeartbeatOps(t: TierData) f64 {
    return t.heartbeat_ops_sec;
}
fn getReplicationOps(t: TierData) f64 {
    return t.replication_ops_sec;
}

const metrics = [_]Metric{
    .{ .name = "propose_ns", .kind = .lower_is_better, .get = getPropose },
    .{ .name = "tick_ns", .kind = .lower_is_better, .get = getTick },
    .{ .name = "tick_parallel_ns", .kind = .lower_is_better, .get = getTickParallel },
    .{ .name = "step_ae_ns", .kind = .lower_is_better, .get = getStepAE },
    .{ .name = "log_append_ns", .kind = .lower_is_better, .get = getLogAppend },
    .{ .name = "ready_advance_ns", .kind = .lower_is_better, .get = getReadyAdvance },
    .{ .name = "heartbeat_ops_sec", .kind = .higher_is_better, .get = getHeartbeatOps },
    .{ .name = "replication_ops_sec", .kind = .higher_is_better, .get = getReplicationOps },
};

// ---------------------------------------------------------------------------
// File reading and parsing
// ---------------------------------------------------------------------------

fn readJsonFile(allocator: std.mem.Allocator, path: []const u8) !BenchResult {
    const file = std.fs.cwd().openFile(path, .{}) catch |err| {
        print("Error: cannot open '{s}': {s}\n", .{ path, @errorName(err) });
        return err;
    };
    defer file.close();

    const content = try file.readToEndAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(content);

    const parsed = std.json.parseFromSlice(BenchResult, allocator, content, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch |err| {
        print("Error: failed to parse '{s}': {s}\n", .{ path, @errorName(err) });
        return err;
    };
    // Intentionally leak — program exits shortly.
    _ = &parsed;

    return parsed.value;
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

fn findTier(result: BenchResult, name: []const u8) ?TierData {
    for (result.tiers) |t| {
        if (std.mem.eql(u8, t.name, name)) return t;
    }
    return null;
}

fn computeDeltaPct(baseline_val: f64, current_val: f64) f64 {
    if (baseline_val == 0) return 0;
    return ((current_val - baseline_val) / baseline_val) * 100.0;
}

fn isRegression(delta_pct: f64, kind: MetricKind, threshold: f64) bool {
    return switch (kind) {
        .higher_is_better => delta_pct < -threshold,
        .lower_is_better => delta_pct > threshold,
    };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var args = std.process.args();
    _ = args.next(); // program name

    const baseline_path = args.next() orelse {
        print("Usage: compare-bench <baseline.json> <current.json> [threshold%]\n", .{});
        std.process.exit(2);
    };
    const current_path = args.next() orelse {
        print("Usage: compare-bench <baseline.json> <current.json> [threshold%]\n", .{});
        std.process.exit(2);
    };

    var threshold: f64 = 20.0;
    if (args.next()) |thr_str| {
        threshold = std.fmt.parseFloat(f64, thr_str) catch 20.0;
    }

    const baseline = readJsonFile(allocator, baseline_path) catch {
        std.process.exit(2);
    };
    const current = readJsonFile(allocator, current_path) catch {
        std.process.exit(2);
    };

    // Print header
    print("\n{s}\n", .{"=" ** 95});
    print("  Raft Benchmark Comparison\n", .{});
    print("  Baseline: {s}\n", .{baseline_path});
    print("  Current:  {s}\n", .{current_path});
    print("  Threshold: {d:.0}%\n", .{threshold});
    print("{s}\n\n", .{"=" ** 95});

    print("  {s:<8} {s:<20} {s:>14} {s:>14} {s:>10} {s:>6}\n", .{
        "Tier", "Metric", "Baseline", "Current", "Delta%", "Status",
    });
    print("  {s:<8} {s:<20} {s:>14} {s:>14} {s:>10} {s:>6}\n", .{
        "-" ** 8, "-" ** 20, "-" ** 14, "-" ** 14, "-" ** 10, "-" ** 6,
    });

    var any_regression = false;

    for (baseline.tiers) |base_tier| {
        if (base_tier.skipped) continue;

        const cur_tier = findTier(current, base_tier.name) orelse continue;
        if (cur_tier.skipped) continue;

        for (metrics) |m| {
            const base_val = m.get(base_tier);
            const cur_val = m.get(cur_tier);

            // Skip metrics that are zero in baseline (not measured for this tier).
            if (base_val == 0) continue;

            const delta = computeDeltaPct(base_val, cur_val);
            const regressed = isRegression(delta, m.kind, threshold);

            if (regressed) any_regression = true;

            const status: []const u8 = if (regressed) "FAIL" else "PASS";
            const sign: []const u8 = if (delta >= 0) "+" else "";

            print("  {s:<8} {s:<20} {d:>14.1} {d:>14.1} {s}{d:>8.1}% {s:>6}\n", .{
                base_tier.name,
                m.name,
                base_val,
                cur_val,
                sign,
                delta,
                status,
            });
        }

        print("\n", .{});
    }

    print("{s}\n", .{"=" ** 95});

    if (any_regression) {
        print("  RESULT: FAIL -- regression(s) detected above threshold\n\n", .{});
        std.process.exit(1);
    } else {
        print("  RESULT: PASS -- all metrics within threshold\n\n", .{});
    }
}
