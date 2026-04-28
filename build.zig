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
        .link_libc = true,
        .imports = imports,
    });
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // -Dwith_otel: compile in otel instrumentation. OFF by default;
    // when off the otel dependency is never resolved and the seam
    // collapses to a comptime-known no-op (verified by
    // otel/scripts/verify-consumer-noop.sh).
    const with_otel = b.option(
        bool,
        "with_otel",
        "Compile in otel instrumentation (off by default; zero overhead when off)",
    ) orelse false;

    const build_options = b.addOptions();
    build_options.addOption(bool, "with_otel", with_otel);
    // Materialize ONE shared `build_options` module. Each consumer module
    // imports the SAME module instance via `addImport` to avoid Zig's
    // "file exists in modules X and X0" error when two independent
    // `addOptions` modules end up sharing a transitive file path.
    const build_options_mod = build_options.createModule();

    // Observability seam (otel). Declared as a SHARED NAMED MODULE so
    // every consumer (lib, tests, examples) resolves to the same source
    // file via `@import("observability")`. The seam's `enabled` flag
    // tracks `build_options.with_otel`; when off, every helper compiles
    // to a comptime-known no-op and the otel dependency is never
    // resolved. The optional `otel` import is wired below inside the
    // `if (with_otel)` block.
    const observability_mod = b.addModule("observability", .{
        .root_source_file = b.path("src/observability.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "build_options", .module = build_options_mod },
        },
    });

    // Thread-pool dependency
    const tp_dep = b.dependency("thread_pool", .{
        .target = target,
        .optimize = optimize,
    });
    const tp_module = tp_dep.module("thread_pool");

    // WAL dependency (write-ahead log)
    const wal_dep = b.dependency("wal", .{
        .target = target,
        .optimize = optimize,
    });
    const wal_module = wal_dep.module("wal");

    // Common import sets used across test modules.
    const raft_lib_imports: []const Import = &.{
        .{ .name = "thread_pool", .module = tp_module },
        .{ .name = "wal", .module = wal_module },
        .{ .name = "observability", .module = observability_mod },
        .{ .name = "build_options", .module = build_options_mod },
    };

    // Main library module
    const module = b.addModule("raft", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = raft_lib_imports,
    });

    // Conditionally wire the otel dependency into the shared
    // `observability` module. When `with_otel == false`, this block does
    // not run and `b.lazyDependency` is never called — so the package
    // manager skips fetching `.otel` from build.zig.zon entirely.
    //
    // We capture the optional bootstrap module so subsequent `addTest`
    // sites that compile `src/root.zig` as their own root (unit tests,
    // bench module, check step) can attach the same import. Gating the
    // attach on `observability_init_mod != null` means flag-off builds
    // never reference the otel namespace from any module.
    var observability_init_mod_opt: ?*std.Build.Module = null;
    if (with_otel) {
        const otel_dep = b.lazyDependency("otel", .{
            .target = target,
            .optimize = optimize,
        }) orelse return;
        const otel_module = otel_dep.module("otel");
        observability_mod.addImport("otel", otel_module);

        // Bootstrap helper. Only registered when `-Dwith_otel=true` so
        // the disabled build never compiles otel-touching code. Gated
        // from src/root.zig via
        // `if (observability.enabled) @import("observability_init")`.
        const observability_init_mod = b.addModule("observability_init", .{
            .root_source_file = b.path("src/observability_init.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "otel", .module = otel_module },
                .{ .name = "observability", .module = observability_mod },
            },
        });
        module.addImport("observability_init", observability_init_mod);
        observability_init_mod_opt = observability_init_mod;
    }

    const raft_imports: []const Import = &.{
        .{ .name = "raft", .module = module },
    };

    // Unit tests (run tests defined in the module itself)
    const test_step = b.step("test", "Run unit tests");
    const unit_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("src/root.zig"), target, optimize, raft_lib_imports),
    });
    // Mirror the `module.addImport("observability_init", ...)` gate so
    // the unit-test root (which is a separate copy of src/root.zig)
    // also resolves the optional `@import("observability_init")` when
    // the flag is on.
    if (observability_init_mod_opt) |m| {
        unit_tests.root_module.addImport("observability_init", m);
    }
    const run_unit_tests = b.addRunArtifact(unit_tests);
    test_step.dependOn(&run_unit_tests.step);

    // Observability disabled-mode no-op tests. Verifies
    // `build_options.with_otel == false` (the default) and that the
    // seam helpers compile to comptime no-ops with stable signatures.
    // Under `-Dwith_otel=true` these tests skip rather than misreport.
    const observability_disabled_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/observability_disabled_test.zig"), target, optimize, &.{
            .{ .name = "raft", .module = module },
            .{ .name = "build_options", .module = build_options_mod },
        }),
    });
    const run_observability_disabled_tests = b.addRunArtifact(observability_disabled_tests);
    test_step.dependOn(&run_observability_disabled_tests.step);

    // Observability enabled-mode smoke test. Skips under
    // `-Dwith_otel=false`; under `-Dwith_otel=true` it installs an
    // in-memory recorder, drives one client-side AppendEntries span and
    // one server-side RequestVote span via the raft seam, and asserts
    // both land in the recorder with span name "raft.{method}".
    const observability_enabled_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/observability_enabled_test.zig"), target, optimize, &.{
            .{ .name = "raft", .module = module },
            .{ .name = "build_options", .module = build_options_mod },
        }),
    });
    if (with_otel) {
        // Re-resolve the otel module under the same `with_otel` guard
        // so the test root sees the same module instance the seam
        // imports. The earlier `lazyDependency` call wired `otel` into
        // `observability_mod`; the test root needs `otel` directly so
        // it can construct the in-memory recorder.
        const otel_dep = b.lazyDependency("otel", .{
            .target = target,
            .optimize = optimize,
        }) orelse return;
        observability_enabled_tests.root_module.addImport("otel", otel_dep.module("otel"));
    }
    const run_observability_enabled_tests = b.addRunArtifact(observability_enabled_tests);
    test_step.dependOn(&run_observability_enabled_tests.step);

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
        "tests/scenarios/parallel.zig",
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
    // option (which defaults to Debug). This includes a ReleaseFast thread-pool
    // dependency so the entire transitive closure is optimized.
    const bench_tp_dep = b.dependency("thread_pool", .{
        .target = target,
        .optimize = .ReleaseFast,
    });
    const bench_wal_dep = b.dependency("wal", .{
        .target = target,
        .optimize = .ReleaseFast,
    });
    const bench_raft_module = b.addModule("raft-bench", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
        .imports = &.{
            .{ .name = "thread_pool", .module = bench_tp_dep.module("thread_pool") },
            .{ .name = "wal", .module = bench_wal_dep.module("wal") },
            // root.zig imports the named "observability" module, so the
            // bench module needs the same name in scope. The seam is a
            // comptime no-op when -Dwith_otel=false (default for benches).
            .{ .name = "observability", .module = observability_mod },
            .{ .name = "build_options", .module = build_options_mod },
        },
    });
    // Match the named "raft" module's optional `observability_init` so
    // src/root.zig resolves under -Dwith_otel=true even when consumed
    // through the bench module.
    if (observability_init_mod_opt) |m| {
        bench_raft_module.addImport("observability_init", m);
    }

    const bench_step = b.step("bench", "Run benchmarks");
    const bench_tests = b.addTest(.{
        .root_module = addRaftModule(b, b.path("tests/bench.zig"), target, .ReleaseFast, &.{
            .{ .name = "raft", .module = bench_raft_module },
        }),
    });
    const run_bench = b.addRunArtifact(bench_tests);
    bench_step.dependOn(&run_bench.step);

    // Incremental scale benchmark executable
    const scale_bench_step = b.step("scale-bench", "Run incremental scale benchmarks with JSON output");
    const scale_bench_exe = b.addExecutable(.{
        .name = "scale-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/scale_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "raft", .module = bench_raft_module },
            },
        }),
    });
    b.installArtifact(scale_bench_exe);
    const run_scale_bench = b.addRunArtifact(scale_bench_exe);
    if (b.args) |args| {
        run_scale_bench.addArgs(args);
    }
    scale_bench_step.dependOn(&run_scale_bench.step);

    // Benchmark comparison tool
    const compare_bench_step = b.step("compare-bench", "Compare benchmark results against a baseline");
    const compare_bench_exe = b.addExecutable(.{
        .name = "compare-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/compare.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const run_compare_bench = b.addRunArtifact(compare_bench_exe);
    if (b.args) |args| {
        run_compare_bench.addArgs(args);
    }
    compare_bench_step.dependOn(&run_compare_bench.step);

    // Example executable (QA-038: compile examples to prevent silent rot)
    const example = b.addExecutable(.{
        .name = "example",
        .root_module = addRaftModule(b, b.path("examples/basic.zig"), target, optimize, &.{}),
    });
    const run_example = b.addRunArtifact(example);
    const example_step = b.step("example", "Run the basic example");
    example_step.dependOn(&run_example.step);

    // --- Example: single-node parallel tick loop --------------------------
    const parallel_example = b.addExecutable(.{
        .name = "single_node_parallel",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/single_node_parallel.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "raft", .module = module },
            },
        }),
    });
    const run_parallel_example = b.addRunArtifact(parallel_example);
    const parallel_example_step = b.step("example-parallel", "Build and run the single-node parallel example");
    parallel_example_step.dependOn(&run_parallel_example.step);

    // Check step for IDE integration (fast syntax validation)
    const check_step = b.step("check", "Check for compilation errors");
    const check = b.addTest(.{
        .root_module = addRaftModule(b, b.path("src/root.zig"), target, optimize, raft_lib_imports),
    });
    if (observability_init_mod_opt) |m| {
        check.root_module.addImport("observability_init", m);
    }
    check_step.dependOn(&check.step);
}
