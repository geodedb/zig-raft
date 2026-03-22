# Quality Gap Analysis

**Last updated:** 2026-03-22
**Scope:** Full codebase quality review -- readability, maintainability, consistency, runtime/build performance, test coverage, documentation, CI/CD.
**Methodology:** 3 parallel review agents (core modules, storage/infra, tests/docs) analyzing all 14 source modules, test suites, build system, CI configuration, documentation, and examples. Phase 3 quality-reviewer validation applied to all findings.

## Summary

| Severity | Total | Resolved | Won't Do | Description |
|----------|-------|----------|----------|-------------|
| High | 5 | 5 | 0 | Stale artifacts, duplicated rejection logic, incomplete Makefile targets, duplicated snapshot serialization, missing AppendEntries member check |
| Medium | 10 | 9 | 1 | Unused helpers, large functions, magic numbers, untested APIs, doc inaccuracies, CI apt-get overhead, no custom CI image |
| Low | 9 | 9 | 0 | CI bench integration, nested overflow readability, build.zig boilerplate, dead code (3), unused imports (2) |

**24 findings total, all resolved (23 Closed + 1 Won't Do).**

### Top 5 Risks (all resolved)

1. **QA-024 (Score 80):** Duplicated snapshot meta serialization -- format change must update two locations or risk silent corruption. **Resolved.**
2. **QA-023b (Score 80, Suspected):** `handleAppendEntries()` missing non-member sender check -- asymmetric security posture vs. other RPC handlers. **Resolved.**
3. **QA-002 (Score 64):** 11 duplicated AppendEntries rejection responses -- high divergence risk in `node.zig`. **Resolved.**
4. **QA-001 (Score 60):** Stale test binaries committed to repo root -- confuses contributors, inflates clone size. **Resolved.**
5. **QA-005 (Score 48):** Makefile aggregate targets skip scenario and chaos tests -- false confidence in CI pass. **Resolved.**

## Scoring

**PriorityScore = Impact(1-5) x Likelihood(1-5) x 4** (max 100)

| Score Range | Severity |
|-------------|----------|
| 60-100 | High |
| 28-59 | Medium |
| 4-27 | Low |

---

## Stack-Ranked Findings

| Rank | ID | Score | Finding | Component | Severity | Category | Issue | Status |
|------|----|-------|---------|-----------|----------|----------|-------|--------|
| 1 | QA-024 | 80 | Duplicated snapshot meta serialization in saveSnapshotImpl() vs writeSnapshotMeta() | `file_storage.zig` | High | maintainability | [#190](https://gitlab.com/devnw/zig/raft/-/issues/190) | **Resolved** |
| 2 | QA-023b | 80 | handleAppendEntries() lacks non-member sender check (suspected) | `node.zig` | High | maintainability | [#193](https://gitlab.com/devnw/zig/raft/-/issues/193) | **Resolved** |
| 3 | QA-002 | 64 | 11 duplicated AppendEntries rejection responses | `node.zig` | High | maintainability | [#171](https://gitlab.com/devnw/zig/raft/-/issues/171) | **Resolved** |
| 4 | QA-001 | 60 | Stale test binaries (test_blake3, test_blake3.o) at repo root | repo root | High | cleanup | [#172](https://gitlab.com/devnw/zig/raft/-/issues/172) | **Resolved** |
| 5 | QA-005 | 48 | Makefile missing test-scenarios, test-chaos in aggregate targets | `Makefile` | High | ci-cd | [#173](https://gitlab.com/devnw/zig/raft/-/issues/173) | **Resolved** |
| 6 | QA-035 | 48 | CI apt-get install runs in every job, wasting 30-60s each | `.gitlab-ci.yml` | Medium | ci-cd | [#194](https://gitlab.com/devnw/zig/raft/-/issues/194) | **Resolved** |
| 7 | QA-036 | 48 | No custom Docker image for CI -- all 7 jobs use debian:bookworm-slim | `.gitlab-ci.yml` | Medium | ci-cd | [#196](https://gitlab.com/devnw/zig/raft/-/issues/196) | **Won't Do** |
| 8 | QA-025 | 40 | 14 repeated createModule blocks in build.zig | `build.zig` | Low | readability | [#198](https://gitlab.com/devnw/zig/raft/-/issues/198) | **Resolved** |
| 9 | QA-003 | 36 | isVoter() exists in membership.zig but unused; 3 inline checks in node.zig (lines 733, 825, 1043) | `node.zig`, `membership.zig` | Medium | maintainability | [#174](https://gitlab.com/devnw/zig/raft/-/issues/174) | **Resolved** |
| 10 | QA-004 | 36 | handleAppendEntries() is 135 lines with 11 exit paths | `node.zig` | Medium | readability | [#175](https://gitlab.com/devnw/zig/raft/-/issues/175) | **Resolved** |
| 11 | QA-007 | 36 | Benchmarks vulnerable to compiler optimization elision | `tests/bench.zig` | Medium | tests | [#176](https://gitlab.com/devnw/zig/raft/-/issues/176) | **Resolved** |
| 12 | QA-010 | 36 | Untested public APIs: stats(), advancePersist(), advanceApply() | `node.zig`, tests | Medium | tests | [#177](https://gitlab.com/devnw/zig/raft/-/issues/177) | **Resolved** |
| 13 | QA-014 | 32 | README claims "Zero Dependencies" but project depends on spice | `README.md` | Medium | docs | [#178](https://gitlab.com/devnw/zig/raft/-/issues/178) | **Resolved** |
| 14 | QA-028 | 28 | hasPendingSnapshot() in log.zig has zero call sites | `log.zig` | Low | cleanup | [#199](https://gitlab.com/devnw/zig/raft/-/issues/199) | **Resolved** |
| 15 | QA-029 | 28 | needsEntries() in replication.zig has zero call sites | `replication.zig` | Low | cleanup | [#200](https://gitlab.com/devnw/zig/raft/-/issues/200) | **Resolved** |
| 16 | QA-030 | 28 | resetPeers() in replication.zig has zero call sites | `replication.zig` | Low | cleanup | [#201](https://gitlab.com/devnw/zig/raft/-/issues/201) | **Resolved** |
| 17 | QA-006 | 24 | Magic number 16 for stack buffer capacity repeated 5 times | `node.zig`, `replication.zig` | Medium | readability | [#179](https://gitlab.com/devnw/zig/raft/-/issues/179) | **Resolved** |
| 18 | QA-008 | 24 | examples/basic.zig is a non-functional stub | `examples/basic.zig` | Medium | docs | [#180](https://gitlab.com/devnw/zig/raft/-/issues/180) | **Resolved** |
| 19 | QA-009 | 24 | WAL recovery doesn't validate entry kind enum value | `file_storage.zig` | Medium | maintainability | [#181](https://gitlab.com/devnw/zig/raft/-/issues/181) | **Resolved** |
| 20 | QA-011 | 24 | integration.zig has only 1 test | `tests/integration.zig` | Medium | tests | [#182](https://gitlab.com/devnw/zig/raft/-/issues/182) | **Resolved** |
| 21 | QA-012 | 24 | CI pipeline missing bench step for regression detection | `.gitlab-ci.yml` | Low | ci-cd | [#183](https://gitlab.com/devnw/zig/raft/-/issues/183) | **Resolved** |
| 22 | QA-026 | 20 | Unused import `state` in election.zig | `election.zig` | Low | cleanup | [#202](https://gitlab.com/devnw/zig/raft/-/issues/202) | **Resolved** |
| 23 | QA-027a | 20 | Unused import `stg` in snapshot.zig | `snapshot.zig` | Low | cleanup | [#203](https://gitlab.com/devnw/zig/raft/-/issues/203) | **Resolved** |
| 24 | QA-013 | 16 | Nested overflow checks in maybeAppend() hard to read | `log.zig` | Low | readability | [#184](https://gitlab.com/devnw/zig/raft/-/issues/184) | **Resolved** |

---

## Findings by Category

### Maintainability (QA-002, QA-003, QA-009, QA-024, QA-023b)

- **QA-024 (High, 80):** `saveSnapshotImpl()` lines 470-487 duplicates `writeSnapshotMeta()` lines 716-735. Both produce identical binary format for snapshot metadata. Extract a shared `serializeSnapshotMeta()` helper. [#190](https://gitlab.com/devnw/zig/raft/-/issues/190)
- **QA-023b (High, 80, Suspected):** `handleAppendEntries()` at line 857 does not validate the sender is a cluster member, while `handleRequestVote()` (line 730) and `handleInstallSnapshot()` (line 1041) both do. Asymmetric security posture. [#193](https://gitlab.com/devnw/zig/raft/-/issues/193)
- **QA-002 (High, 64):** 11 nearly identical AppendEntries rejection responses in `handleAppendEntries()`. Extract a `rejectAppendEntries()` helper. [#171](https://gitlab.com/devnw/zig/raft/-/issues/171)
- **QA-003 (Medium, 36):** `membership.zig` defines `isVoter()` but `node.zig` uses inline voter checks at 3 locations (lines 733, 825, 1043) instead. Replace all with the existing helper. [#174](https://gitlab.com/devnw/zig/raft/-/issues/174)
- **QA-009 (Medium, 24):** WAL recovery reads entry `kind` byte without validating it's a valid enum member. Use `std.meta.intToEnum()` or explicit range check. [#181](https://gitlab.com/devnw/zig/raft/-/issues/181)

### Readability (QA-004, QA-006, QA-013, QA-025)

- **QA-004 (Medium, 36):** `handleAppendEntries()` spans 135 lines with 11 exit paths. Decompose into term validation, log consistency, conflict resolution, and entry application phases. [#175](https://gitlab.com/devnw/zig/raft/-/issues/175)
- **QA-025 (Low, 40):** `build.zig` has 14 `b.createModule(...)` blocks with identical structure. Extract a helper function to reduce boilerplate from ~233 to ~120 lines. [#198](https://gitlab.com/devnw/zig/raft/-/issues/198)
- **QA-006 (Medium, 24):** Magic number `16` for stack buffer capacity appears 5 times across `node.zig` and `replication.zig`. Extract a named constant. [#179](https://gitlab.com/devnw/zig/raft/-/issues/179)
- **QA-013 (Low, 16):** Nested `@addWithOverflow` checks in `maybeAppend()` are hard to parse. Extract a `checkedAddIndex()` helper. [#184](https://gitlab.com/devnw/zig/raft/-/issues/184)

### Tests (QA-007, QA-010, QA-011)

- **QA-007 (Medium, 36):** Benchmarks compiled with `ReleaseFast` don't use `std.mem.doNotOptimizeAway()`, risking compiler elision of benchmark bodies. [#176](https://gitlab.com/devnw/zig/raft/-/issues/176)
- **QA-010 (Medium, 36):** New PERF-006/PERF-020 public APIs (`stats()`, `advancePersist()`, `advanceApply()`) lack dedicated test coverage. [#177](https://gitlab.com/devnw/zig/raft/-/issues/177)
- **QA-011 (Medium, 24):** `integration.zig` has only 1 test. Multi-node lifecycle, failover, and snapshot scenarios should be added. [#182](https://gitlab.com/devnw/zig/raft/-/issues/182)

### CI/CD (QA-005, QA-012, QA-035, QA-036)

- **QA-005 (High, 48):** `make test` and `make ci` skip scenario tests and chaos tests. Wire `test-scenarios` and `test-chaos` into aggregate targets. [#173](https://gitlab.com/devnw/zig/raft/-/issues/173)
- **QA-035 (Medium, 48):** `.install-zig` anchor runs `apt-get update && apt-get install` in every job (~30-60s each, 7 jobs). Skip when Zig is already cached. [#194](https://gitlab.com/devnw/zig/raft/-/issues/194)
- **QA-036 (Medium, 48):** All 7 CI jobs use `debian:bookworm-slim`. A custom Docker image with Zig pre-installed would eliminate per-job overhead entirely. [#196](https://gitlab.com/devnw/zig/raft/-/issues/196)
- **QA-012 (Low, 24):** Benchmarks exist but aren't in CI. Add an optional pipeline job for automated regression detection. [#183](https://gitlab.com/devnw/zig/raft/-/issues/183)

### Docs (QA-008, QA-014)

- **QA-014 (Medium, 32):** README states "Zero Dependencies" but the project depends on `spice`. Update to reflect actual dependency. [#178](https://gitlab.com/devnw/zig/raft/-/issues/178)
- **QA-008 (Medium, 24):** `examples/basic.zig` is a stub with commented-out code referencing a non-existent API. Replace with a working ready-state loop example. [#180](https://gitlab.com/devnw/zig/raft/-/issues/180)

### Cleanup (QA-001, QA-026, QA-027a, QA-028, QA-029, QA-030)

- **QA-001 (High, 60):** `test_blake3` (5.5k) and `test_blake3.o` (23k) are stale build artifacts committed to the repo root. Remove and add to `.gitignore`. [#172](https://gitlab.com/devnw/zig/raft/-/issues/172)
- **QA-028 (Low, 28):** `hasPendingSnapshot()` at `log.zig:351` is public but has zero call sites. Remove or add test with documented use case. [#199](https://gitlab.com/devnw/zig/raft/-/issues/199)
- **QA-029 (Low, 28):** `needsEntries()` at `replication.zig:52` is public but has zero call sites. Remove or integrate into callers. [#200](https://gitlab.com/devnw/zig/raft/-/issues/200)
- **QA-030 (Low, 28):** `resetPeers()` at `replication.zig:82` is public but has zero call sites. Duplicates `init()` logic. Remove or adopt. [#201](https://gitlab.com/devnw/zig/raft/-/issues/201)
- **QA-026 (Low, 20):** Unused import `state` at `election.zig:5`. Remove the line. [#202](https://gitlab.com/devnw/zig/raft/-/issues/202)
- **QA-027a (Low, 20):** Unused import `stg` at `snapshot.zig:6`. Remove the line. [#203](https://gitlab.com/devnw/zig/raft/-/issues/203)

---

## Dismissed Findings

The following scanner findings were reviewed and dismissed during Phase 3 validation:

- **QA-027 (original):** `membership` import in `snapshot.zig` -- confirmed used at lines 278-279. Not dead.
- **QA-031:** `serializeVoters()` -- intentional public API for consumers. Not dead code.
- **QA-032:** `stats()` -- intentional public API for consumers. Not dead code.

---

## Roadmap

All phases complete. 23 findings resolved, 1 Won't Do.

### Phase 1: Quick Wins (< 1 day) -- Complete

| ID | Effort | Action | Status |
|----|--------|--------|--------|
| QA-001 | Trivial | `git rm test_blake3*`, add to `.gitignore` | **Resolved** |
| QA-005 | Low | Update Makefile aggregate targets | **Resolved** |
| QA-014 | Trivial | Fix README dependency claim | **Resolved** |
| QA-006 | Low | Extract `MAX_STACK_PEERS` constant | **Resolved** |
| QA-026 | Trivial | Remove unused `state` import in `election.zig` | **Resolved** |
| QA-027a | Trivial | Remove unused `stg` import in `snapshot.zig` | **Resolved** |
| QA-028 | Trivial | Remove dead `hasPendingSnapshot()` or add test | **Resolved** |
| QA-029 | Trivial | Remove dead `needsEntries()` or integrate | **Resolved** |
| QA-030 | Trivial | Remove dead `resetPeers()` or integrate | **Resolved** |
| QA-035 | Low | Skip `apt-get` when Zig is cached in CI | **Resolved** |

### Phase 2: Refactoring (1-3 days) -- Complete

| ID | Effort | Action | Status |
|----|--------|--------|--------|
| QA-024 | Small | Extract shared `serializeSnapshotMeta()` helper in `file_storage.zig` | **Resolved** |
| QA-023b | Small | Add non-member sender check to `handleAppendEntries()` | **Resolved** |
| QA-002 | Medium | Extract `rejectAppendEntries()` helper | **Resolved** |
| QA-004 | Medium | Decompose `handleAppendEntries()` | **Resolved** |
| QA-003 | Low | Replace 3 inline voter checks with `isVoter()` calls | **Resolved** |
| QA-009 | Low | Add `intToEnum` validation in WAL recovery | **Resolved** |
| QA-025 | Small | Extract helper for `createModule` blocks in `build.zig` | **Resolved** |

### Phase 3: Test Coverage (1-3 days) -- Complete

| ID | Effort | Action | Status |
|----|--------|--------|--------|
| QA-010 | Medium | Add tests for stats/advancePersist/advanceApply | **Resolved** |
| QA-007 | Low | Add `doNotOptimizeAway` to benchmarks | **Resolved** |
| QA-011 | Medium | Expand integration test suite | **Resolved** |
| QA-008 | Medium | Write working example | **Resolved** |

### Phase 4: CI Hardening (3-5 days) -- Complete

| ID | Effort | Action | Status |
|----|--------|--------|--------|
| QA-036 | Medium | Build and publish custom CI Docker image | **Won't Do** -- requires external Docker registry infrastructure |
| QA-012 | Medium | Add bench job to CI pipeline | **Resolved** |
| QA-013 | Low | Simplify overflow checks in maybeAppend | **Resolved** |
