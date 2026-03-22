# Gap Analysis

**Last updated:** 2026-03-21
**Spec:** `docs/superpowers/specs/2026-03-21-raft-design.md`
**Methodology:** Requirements-to-code audit — all source, tests, CI, and docs compared against design spec.

## Summary

| Severity | Open | Resolved | Won't Do |
|----------|------|----------|----------|
| Critical | 0 | 0 | 0 |
| High | 0 | 1 | 0 |
| Medium | 0 | 5 | 0 |
| Low | 0 | 2 | 0 |

All 150 tests pass (104 unit, 1 integration, 34 scenario, 11 chaos). Zero TODO/FIXME markers in source. All gaps have been resolved. See [GAP_ANALYSIS_RESOLVED.md](GAP_ANALYSIS_RESOLVED.md) for details.

## Won't Do — Handled via External Controls

The following items from the design spec are intentionally not addressed in this repository because they are handled via separate organizational controls:

- **CI/CD image hash pinning:** Container image versions and hash pinning are managed centrally via the [devnw/ci-catalog](https://gitlab.com/devnw/ci-catalog) component templates. Individual repositories consume pinned images transitively.
- **Docker tag pinning:** Same as above — managed at the CI catalog level, not per-repository.
- **Pre-commit hooks:** Code quality checks (formatting, linting) are enforced via CI pipeline stages and developer tooling configurations external to this repository (editor configs, Makefile targets like `make dev`).

---

## Open Gaps

*None — all gaps resolved.*

## Geode Extraction Metadata

This repository is the right home for the reusable Raft core, but the Geode review found a clear distinction between the portable consensus engine and Geode-specific transport/persistence/transaction adapters.

### Geode Boundary

Move/own here:

| Geode source | Why it belongs in `raft` |
|---|---|
| `src/distributed/raft.zig:38`, `:45`, `:64`, `:146`, `:157` | Core node state, log entry, message, config, and node types |
| `src/distributed/raft.zig:188`, `:242`, `:254`, `:285`, `:297`, `:323`, `:363`, `:399`, `:405`, `:459`, `:550`, `:650`, `:716` | Portable Raft lifecycle, ticking, message handling, proposal, election, append-entries, snapshot handling, and commit advancement |
| `src/distributed/raft.zig:754`, `:768`, `:788` | Direct source-level core tests |
| `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` | Cluster-level election, replication, partition recovery, and commit advancement behavior |

Keep in Geode or split into adapter code:

| Geode source | Why it stays |
|---|---|
| `src/distributed/raft_consensus.zig:62`, `:87`, `:94` | Transaction prepare/decide, schema, and snapshot-pointer commands are Geode-specific distributed behavior layered on top of Raft |
| `src/distributed/raft_consensus.zig:135` through `:211` | QUIC transport/client abstractions are application integration, not the Raft core |
| `src/distributed/raft_consensus.zig:215` through `:367` | Persistent storage integration is an adapter boundary, not the consensus core itself |
| `src/distributed/raft_consensus.zig:412` through `:457` | Protobuf frame encoding/decoding is transport framing glue |
| `build.zig:431`, `:432`, `:433`, `:434` | `raft_schema` protobuf wiring is Geode build integration |
| `src/dist/scaffold.zig:15` through `:259` | Alternate scaffold implementation and 2PC/shard-map helpers are not the core extraction target |

### Geode Tests And Benchmarks

| Geode reference | Purpose |
|---|---|
| `src/distributed/raft.zig:754`, `:768`, `:788` | Core unit coverage |
| `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` | End-to-end cluster behavior |
| `src/tools/run_variance_benches.zig:300`, `:301`, `:308`, `:312`, `:314`, `:370`, `:398`, `:465` | Current synthetic variance benchmark reference |

### Current Assessment

- Reusable core: `src/distributed/raft.zig`.
- Non-core Geode adjacency: `raft_consensus.zig`, protobuf framing, QUIC transport, persistent storage adapter, and transaction/schema commands.
- Migration priority: keep the consensus node and message/state machine in this repo; treat the rest as explicit adapters.
