// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Spice dependency (fork/join parallelism)
    const spice_dep = b.dependency("spice", .{
        .target = target,
        .optimize = optimize,
    });
    const spice_module = spice_dep.module("spice");

    // Main library module
    const module = b.addModule("raft", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "spice", .module = spice_module },
        },
    });

    // Unit tests (run tests defined in the module itself)
    const test_step = b.step("test", "Run unit tests");
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spice", .module = spice_module },
            },
        }),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);

    // Test harness module (shared by integration and chaos tests)
    const harness_module = b.createModule(.{
        .root_source_file = b.path("tests/harness.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "raft", .module = module },
        },
    });

    // Checker module (shared by scenario tests)
    const checker_module = b.createModule(.{
        .root_source_file = b.path("tests/checker.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "raft", .module = module },
        },
    });

    // Integration tests (separate test file with module import)
    const integration_step = b.step("test-integration", "Run integration tests");
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
                .{ .name = "harness", .module = harness_module },
            },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    integration_step.dependOn(&run_integration_tests.step);

    // Scenario tests (snapshot, etc.)
    const scenarios_step = b.step("test-scenarios", "Run scenario tests");
    const snapshot_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/scenarios/snapshots.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
                .{ .name = "harness", .module = harness_module },
                .{ .name = "checker", .module = checker_module },
            },
        }),
    });
    const run_snapshot_tests = b.addRunArtifact(snapshot_tests);
    scenarios_step.dependOn(&run_snapshot_tests.step);

    const election_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/scenarios/election.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
                .{ .name = "harness", .module = harness_module },
                .{ .name = "checker", .module = checker_module },
            },
        }),
    });
    const run_election_tests = b.addRunArtifact(election_tests);
    scenarios_step.dependOn(&run_election_tests.step);

    const persistence_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/scenarios/persistence.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
                .{ .name = "harness", .module = harness_module },
            },
        }),
    });
    const run_persistence_tests = b.addRunArtifact(persistence_tests);
    scenarios_step.dependOn(&run_persistence_tests.step);

    const file_storage_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/scenarios/file_storage.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
            },
        }),
    });
    const run_file_storage_tests = b.addRunArtifact(file_storage_tests);
    scenarios_step.dependOn(&run_file_storage_tests.step);

    // Chaos tests with optional seed for deterministic replay
    const chaos_step = b.step("test-chaos", "Run chaos tests");
    const chaos_seed = b.option(u64, "seed", "Deterministic seed for chaos test replay");
    const chaos_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/chaos.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft", .module = module },
                .{ .name = "harness", .module = harness_module },
            },
        }),
    });
    const chaos_options = b.addOptions();
    chaos_options.addOption(?u64, "seed", chaos_seed);
    chaos_tests.root_module.addOptions("chaos_options", chaos_options);
    const run_chaos_tests = b.addRunArtifact(chaos_tests);
    chaos_step.dependOn(&run_chaos_tests.step);

    // Check step for IDE integration (fast syntax validation)
    const check_step = b.step("check", "Check for compilation errors");
    const check = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spice", .module = spice_module },
            },
        }),
    });
    check_step.dependOn(&check.step);
}
