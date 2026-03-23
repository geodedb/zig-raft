// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");

/// Import definition for addRaftModule helper.
const Import = std.Build.Module.Import;

/// Helper to reduce repeated b.createModule(...) boilerplate (QA-025).
/// Encapsulates the common pattern of creating a module with target, optimize, and imports.
fn addRaftModule(
    b: *std.Build,
    root_source_file: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    imports: []const Import,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = root_source_file,
        .target = target,
        .optimize = optimize,
        .imports = imports,
    });
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Spice dependency (fork/join parallelism)
    const spice_dep = b.dependency("spice", .{
        .target = target,
        .optimize = optimize,
    });
    const spice_module = spice_dep.module("spice");

    // Common import sets used across test modules.
    const spice_imports: []const Import = &.{
        .{ .name = "spice", .module = spice_module },
    };

    // Main library module
    const module = b.addModule("raft", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = spice_imports,
    });

    const raft_imports: []const Import = &.{
        .{ .name = "raft", .module = module },
    };

    // Unit tests (run tests defined in the module itself)
    const test_step = b.step("test", "Run unit tests");
    const unit_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("src/root.zig"), target, optimize, spice_imports),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);

    // Test harness module (shared by integration and chaos tests)
    const harness_module = addRaftModule(b, b.path("tests/harness.zig"), target, optimize, raft_imports);

    // Checker module (shared by scenario tests)
    const checker_module = addRaftModule(b, b.path("tests/checker.zig"), target, optimize, raft_imports);

    // Common scenario test imports (raft + harness + checker).
    const scenario_imports: []const Import = &.{
        .{ .name = "raft", .module = module },
        .{ .name = "harness", .module = harness_module },
        .{ .name = "checker", .module = checker_module },
    };

    // Integration tests (separate test file with module import)
    const integration_step = b.step("test-integration", "Run integration tests");
    const integration_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/integration.zig"), target, optimize, &.{
            .{ .name = "raft", .module = module },
            .{ .name = "harness", .module = harness_module },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    integration_step.dependOn(&run_integration_tests.step);

    // Scenario tests (snapshot, etc.)
    const scenarios_step = b.step("test-scenarios", "Run scenario tests");

    const scenario_files = [_][]const u8{
        "tests/scenarios/snapshots.zig",
        "tests/scenarios/election.zig",
        "tests/scenarios/persistence.zig",
        "tests/scenarios/membership.zig",
        "tests/scenarios/replication.zig",
        "tests/scenarios/adversarial.zig",
    };

    for (scenario_files) |path| {
        const t = b.addTest(.{
            .root_module = addRaftModule(b, b.path(path), target, optimize, scenario_imports),
        });
        const run = b.addRunArtifact(t);
        scenarios_step.dependOn(&run.step);
    }

    // file_storage scenario uses raft + checker (no harness).
    const file_storage_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/scenarios/file_storage.zig"), target, optimize, &.{
            .{ .name = "raft", .module = module },
            .{ .name = "checker", .module = checker_module },
        }),
    });
    const run_file_storage_tests = b.addRunArtifact(file_storage_tests);
    scenarios_step.dependOn(&run_file_storage_tests.step);

    // Chaos tests with optional seed for deterministic replay
    const chaos_step = b.step("test-chaos", "Run chaos tests");
    const chaos_seed = b.option(u64, "seed", "Deterministic seed for chaos test replay");
    const chaos_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/chaos.zig"), target, optimize, scenario_imports),
    });
    const chaos_options = b.addOptions();
    chaos_options.addOption(?u64, "seed", chaos_seed);
    chaos_tests.root_module.addOptions("chaos_options", chaos_options);
    const run_chaos_tests = b.addRunArtifact(chaos_tests);
    chaos_step.dependOn(&run_chaos_tests.step);

    // Benchmarks (PERF-016)
    // PERF-027: Create a separate ReleaseFast raft module for benchmarks so
    // the raft dependency is compiled with ReleaseFast, not the CLI optimize
    // option (which defaults to Debug). This includes a ReleaseFast spice
    // dependency so the entire transitive closure is optimized.
    const bench_spice_dep = b.dependency("spice", .{
        .target = target,
        .optimize = .ReleaseFast,
    });
    const bench_raft_module = b.addModule("raft-bench", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "spice", .module = bench_spice_dep.module("spice") },
        },
    });

    const bench_step = b.step("bench", "Run benchmarks");
    const bench_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/bench.zig"), target, .ReleaseFast, &.{
            .{ .name = "raft", .module = bench_raft_module },
        }),
    });
    const run_bench = b.addRunArtifact(bench_tests);
    bench_step.dependOn(&run_bench.step);

    // Example executable (QA-038: compile examples to prevent silent rot)
    const example = b.addExecutable(.{
        .name = "example-basic",
        .root_module = addRaftModule(b, b.path("examples/basic.zig"), target, optimize, &.{}),
    });
    const run_example = b.addRunArtifact(example);
    const example_step = b.step("example", "Run the basic example");
    example_step.dependOn(&run_example.step);

    // Check step for IDE integration (fast syntax validation)
    const check_step = b.step("check", "Check for compilation errors");
    const check = b.addTest(.{
        .root_module = addRaftModule(b, b.path("src/root.zig"), target, optimize, spice_imports),
    });
    check_step.dependOn(&check.step);
}
