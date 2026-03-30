# raft - Zig Library Makefile
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2024-2025 CodePros

.PHONY: all build test test-unit test-integration test-scenarios test-chaos clean check fmt help dev ci bench
.PHONY: scale-bench bench-save bench-compare bench-baseline
.PHONY: test-release-safe test-release-fast test-release-small test-all-optimizations
.PHONY: pre-commit-install pre-commit pre-commit-run

# Default target: build and run tests
all: build test-unit
	@echo "✅ All builds and tests completed successfully!"

# Build the library
build:
	@echo "🔨 Building raft..."
	@zig build

# Run all tests (unit + integration + scenarios + chaos)
test: test-unit test-integration test-scenarios test-chaos
	@echo "✅ All tests completed"

# Run unit tests only
test-unit:
	@echo "🧪 Running unit tests..."
	@zig build test --summary all

# Run integration tests only
test-integration:
	@echo "🔗 Running integration tests..."
	@zig build test-integration --summary all

# Run scenario tests
test-scenarios:
	@echo "🧪 Running scenario tests..."
	@zig build test-scenarios --summary all

# Run chaos tests
test-chaos:
	@echo "🧪 Running chaos tests..."
	@zig build test-chaos --summary all

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf zig-cache zig-out .zig-cache

# Check for compilation errors (fast, no codegen)
check:
	@echo "🔍 Checking for compilation errors..."
	@zig build check

# Format source code
fmt:
	@echo "📝 Formatting code..."
	@zig fmt src/ tests/ build.zig

# Quick development iteration loop (check removed — test-unit compiles the
# same code, so running both wastes ~2-3s per iteration; PERF-019).
dev: fmt test-unit
	@echo "✅ Development checks passed"

# Full CI pipeline (clean slate -- includes all test suites)
ci: clean build test
	@echo "✅ CI pipeline passed"

# Run benchmarks (ReleaseFast)
bench:
	@echo "Running benchmarks (ReleaseFast)..."
	@zig build bench --summary all

# Incremental scale benchmark (JSON to stdout, table to stderr)
scale-bench:
	@echo "Running incremental scale benchmark..."
	@zig build scale-bench 2>/dev/null

# Run scale bench and save results to benchmarks/results/<git-sha>.json
bench-save:
	@echo "Running scale benchmark and saving results..."
	@zig build scale-bench 2>/dev/null > benchmarks/results/$$(git rev-parse --short HEAD).json
	@echo "Saved to benchmarks/results/$$(git rev-parse --short HEAD).json"

# Compare current results against baseline
bench-compare:
	@echo "Running scale benchmark and comparing against baseline..."
	@zig build scale-bench 2>/dev/null > /tmp/raft-current-bench.json
	@zig build compare-bench -- benchmarks/results/baseline.json /tmp/raft-current-bench.json

# Save current results as the new baseline
bench-baseline:
	@echo "Running scale benchmark and saving as baseline..."
	@zig build scale-bench 2>/dev/null > benchmarks/results/baseline.json
	@echo "Saved to benchmarks/results/baseline.json"

# Release optimization level tests
test-release-safe:
	@echo "🧪 Testing with ReleaseSafe..."
	@zig build test -Doptimize=ReleaseSafe --summary all

test-release-fast:
	@echo "🧪 Testing with ReleaseFast..."
	@zig build test -Doptimize=ReleaseFast --summary all

test-release-small:
	@echo "🧪 Testing with ReleaseSmall..."
	@zig build test -Doptimize=ReleaseSmall --summary all

test-all-optimizations: test-unit test-release-safe test-release-fast test-release-small
	@echo "✅ All optimization levels passed"

# Pre-commit hook management
pre-commit-install:
	@echo "📦 Installing pre-commit hooks..."
	@pre-commit install
	@pre-commit install --hook-type commit-msg
	@pre-commit install --hook-type pre-push

pre-commit:
	@echo "🔍 Running pre-commit checks..."
	@pre-commit run --all-files

pre-commit-run: pre-commit

# Display help
help:
	@echo "raft - Zig Library"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Development:"
	@echo "  all                  Build and run tests (default)"
	@echo "  build                Build the library"
	@echo "  dev                  Quick iteration: fmt + test"
	@echo "  ci                   Full CI: clean + build + test"
	@echo ""
	@echo "Testing:"
	@echo "  test                 Run all tests (unit + integration + scenarios + chaos)"
	@echo "  test-unit            Run unit tests only"
	@echo "  test-integration     Run integration tests only"
	@echo "  test-scenarios       Run scenario tests"
	@echo "  test-chaos           Run chaos tests"
	@echo "  test-release-safe    Test with ReleaseSafe"
	@echo "  test-release-fast    Test with ReleaseFast"
	@echo "  test-release-small   Test with ReleaseSmall"
	@echo "  test-all-optimizations  Test all optimization levels"
	@echo "  bench                Run benchmarks (ReleaseFast)"
	@echo "  scale-bench          Run scale benchmarks (JSON to stdout)"
	@echo "  bench-save           Save scale bench results to results/<sha>.json"
	@echo "  bench-compare        Compare scale bench against baseline"
	@echo "  bench-baseline       Save current scale bench as baseline"
	@echo ""
	@echo "Code Quality:"
	@echo "  fmt                  Format source code"
	@echo "  check                Check for compilation errors"
	@echo "  pre-commit           Run pre-commit hooks"
	@echo "  pre-commit-install   Install pre-commit hooks"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean                Remove build artifacts"
	@echo "  help                 Show this help message"
