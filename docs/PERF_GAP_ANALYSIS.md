# Performance Gap Analysis

**Last updated:** 2026-03-22
**Scope:** Full codebase performance review -- algorithmic hot paths, memory allocations, I/O patterns, parallelism, CI/CD pipeline, missing benchmarks.
**Methodology:** Pass 1: 4 parallel review agents (algorithmic hot paths, memory allocations, I/O + parallelism, build + CI) analyzing all 14 source modules, tests, build system, and CI configuration. Pass 2: targeted review of benchmark infrastructure, init paths, and FileStorage allocation patterns.

## Summary

| PerfSeverity | Total | Resolved | Deferred | Open | Description |
|--------------|-------|----------|----------|------|-------------|
| P0 | 1 | 1 | 0 | 0 | SLO/hot-path risk -- heap allocations + quadratic loop in commit path |
| P1 | 6 | 5 | 1 | 0 | Material user-facing -- double allocations, uncached peer lists, sequential pipeline, benchmark Debug mode |
| P2 | 11 | 11 | 0 | 0 | Moderate -- I/O batching, vtable overhead, CI efficiency, missing benchmarks, FileStorage benchmarks |
| P3 | 5 | 5 | 0 | 0 | Minor -- clearAndFree, redundant AE build, make dev, double loadSnapshot, per-call buffer alloc |
| Info | 1 | 1 | 0 | 0 | Hardening -- missing performance metrics |

**24 findings total (23 resolved, 1 deferred, 0 open).** The single deferred item (PERF-005) requires a Storage vtable interface change for a test-only optimization -- deferred until the next major API revision.

### Remaining Deferred

| Rank | ID | PerfScore | Risk | Expected Impact |
|------|----|-----------|------|-----------------|
| 1 | PERF-005 | 60 | Entry data duplicated twice on follower path | 2x memory for InMemoryStorage (test-only, deferred until next major API revision) |

## Baseline Measurements

| Test Suite | Duration | MaxRSS | Tests |
|------------|----------|--------|-------|
| Unit tests | 14ms (run) | 2M | 127 |
| Integration | 1ms (run) | 2M | 3 |
| Scenarios | 17ms (max run) | 3M | 49 |
| Chaos | 143ms (run) | 13M | 11 |
| Benchmarks | - | - | 8 |
| **Total compile+run** | **~0.6s** | **38M (compile)** | **198** |

Benchmarks available via `zig build bench --summary all` (PERF-016). 8 benchmarks: 5 hot-path (propose-ready-advance, log-append-64, log-slice-unstable, leader-tick, stableTo) + 3 FileStorage (PERF-025). All benchmarks now compile with ReleaseFast (PERF-027 resolved).

---

## Stack-Ranked Findings

| Rank | Sev | Score | Finding | Component | Evidence | Issue | Fix Summary | Status |
|------|-----|-------|---------|-----------|----------|-------|-------------|--------|
| 1 | P0 | 100 | Heap allocs + O(V*N) in commit path -- 4-6 alloc/free per maybeCommit + quadratic gather in committedForSet | `replication.zig:188-216`, `quorum.zig:34-59` | Stack buffers [16]u64 in maybeCommit + committedForSet; eliminates all heap allocs for typical clusters | [#144](https://gitlab.com/devnw/zig/raft/-/issues/144) | Stack buffers [16]u64, pass HashMap directly | Resolved |
| 2 | P1 | 80 | Double log.slice() in ready/advance -- same committed entries fetched twice with allocation | `node.zig:392-408,437-441` | advance() now scans ready_state.pending_committed_entries instead of re-slicing the log | [#145](https://gitlab.com/devnw/zig/raft/-/issues/145) | Scan pending_committed_entries in advance() | Resolved |
| 3 | P1 | 80 | log.slice() always allocates for unstable-only reads -- copies contiguous in-memory data | `log.zig:103-113` | borrowUnstable() returns non-owning view; ready() uses borrow path first, falls back to slice() | [#146](https://gitlab.com/devnw/zig/raft/-/issues/146) | borrowUnstable() zero-copy + ready() borrow-first | Resolved |
| 4 | P1 | 80 | Benchmark raft module compiled in Debug mode -- all 5 benchmark ns/op numbers are 5-20x slower than production | `build.zig:18-25`, `tests/bench.zig` | Shared raft module uses CLI `optimize` (default Debug); bench root is ReleaseFast but imported raft code is Debug | [#188](https://gitlab.com/devnw/zig/raft/-/issues/188) | Create separate ReleaseFast raft module for bench, or add comptime assert | Resolved |
| 5 | P1 | 60 | dynamicPeers() allocates per heartbeat -- fresh []u64 on every call | `node.zig:194-239` | Stack buffer [16]u64 on RaftNode struct; heap alloc only for >16 peers | [#147](https://gitlab.com/devnw/zig/raft/-/issues/147) | Stack buffer on struct, heap fallback for >16 peers | Resolved |
| 6 | P1 | 60 | Entry data duplicated twice on follower path -- log.append dupe + storage dupe | `log.zig:49-63`, `storage.zig:136-148` | Only affects InMemoryStorage (test backend); FileStorage serializes to disk without duping | [#148](https://gitlab.com/devnw/zig/raft/-/issues/148) | Deferred -- requires Storage vtable interface change for test-only impact | Deferred |
| 7 | P1 | 48 | advance() forces sequential persist+send -- no pipelining possible | `node.zig:419-477` | Split into advancePersist() + advanceApply(); advance() calls both for backward compat | [#149](https://gitlab.com/devnw/zig/raft/-/issues/149) | Split into advancePersist/advanceApply | Resolved |
| 8 | P2 | 48 | Per-entry WAL writes -- 2N syscalls instead of 1 batched write | `file_storage.zig:277-348` | Single batch buffer for all entries; single writeAll + single sync; pre-reserved index capacity | [#150](https://gitlab.com/devnw/zig/raft/-/issues/150) | Single buffer + single writeAll for entire batch | Resolved |
| 9 | P2 | 48 | WAL compaction reads/writes one-at-a-time -- ~2N syscalls | `file_storage.zig:599-681` | Bulk read of remaining bytes + single writeAll to temp WAL; zero decode/re-encode | [#151](https://gitlab.com/devnw/zig/raft/-/issues/151) | Bulk byte-copy of remaining WAL range | Resolved |
| 10 | P2 | 48 | No FileStorage benchmarks -- zero regression detection for WAL write, bulk read, compaction, snapshot persist, recovery | `tests/bench.zig` | All 5 benchmarks use InMemoryStorage; no FileStorage import or usage | [#192](https://gitlab.com/devnw/zig/raft/-/issues/192) | Add 3 FileStorage benchmarks (append, read, snapshot) | Resolved |
| 11 | P2 | 36 | O(n) shift in stableTo/compact -- copyForwards on every advance | `log.zig:206-222,238-258` | unstable_start offset advances instead of shifting; maybeCompactUnstable defers shift | [#152](https://gitlab.com/devnw/zig/raft/-/issues/152) | Start-offset tracking with deferred compaction | Resolved |
| 12 | P2 | 36 | lastIndex() vtable call when unstable empty -- frequent on followers | `log.zig:65-69` | Added `cached_storage_last_index` field; updated in stableTo/restore; eliminates vtable dispatch | [#153](https://gitlab.com/devnw/zig/raft/-/issues/153) | Cache last_index field on RaftLog | Resolved |
| 13 | P2 | 36 | FileStorage.entriesImpl per-entry seeks -- N seek+read instead of 1 bulk read | `file_storage.zig:351-379` | Single seekTo + readAll of contiguous byte range; parse entries from buffer | [#154](https://gitlab.com/devnw/zig/raft/-/issues/154) | Bulk readAll of contiguous byte range | Resolved |
| 14 | P2 | 32 | sendHeartbeats parallel arrays heap-allocated per tick | `node.zig:958-961` | Stack buffers [16]BuildAEFuture + [16]?rpc.Message; heap fallback for >16 peers | [#155](https://gitlab.com/devnw/zig/raft/-/issues/155) | Stack-allocated [16]T buffers | Resolved |
| 15 | P2 | 32 | WAL recovery reads each record twice (header then full) | `file_storage.zig:857-927` | Single bulk readAll of entire WAL into memory; parse records from buffer | [#156](https://gitlab.com/devnw/zig/raft/-/issues/156) | Bulk readAll of entire WAL into memory | Resolved |
| 16 | P2 | 32 | CI downloads Zig 7 times per pipeline | `.gitlab-ci.yml` | Added `default: cache:` with key `zig-${ZIG_VERSION}`; conditional download skips if cached | [#157](https://gitlab.com/devnw/zig/raft/-/issues/157) | GitLab cache + conditional download | Resolved |
| 17 | P2 | 24 | CI test jobs blocked on build without using artifacts | `.gitlab-ci.yml` | Removed `needs: [build]` from all test jobs; stage ordering still enforced | [#158](https://gitlab.com/devnw/zig/raft/-/issues/158) | Remove needs from test jobs | Resolved |
| 18 | P2 | 24 | No benchmarks exist -- zero regression detection | build.zig, Makefile | `zig build bench` + `make bench`; 5 benchmarks: propose-ready-advance, log-append-64, log-slice-unstable, leader-tick, stableTo | [#159](https://gitlab.com/devnw/zig/raft/-/issues/159) | tests/bench.zig + build.zig bench step + Makefile target | Resolved |
| 19 | P3 | 24 | Double loadSnapshot() on init -- RaftLog.init loads+frees snapshot, then RaftNode.init loads again for config | `log.zig:24-34`, `node.zig:134-137` | With FileStorage: double disk read + double BLAKE3 hash on every node restart | [#195](https://gitlab.com/devnw/zig/raft/-/issues/195) | Pass pre-loaded snapshot to RaftLog.init or return metadata from init | Resolved |
| 20 | P3 | 16 | resetVotes uses clearAndFree instead of clearRetainingCapacity | `election.zig:119` | Frees + reallocates HashMap backing on every election start | [#160](https://gitlab.com/devnw/zig/raft/-/issues/160) | One-line change to clearRetainingCapacity | Resolved |
| 21 | P3 | 12 | Per-call batch buffer alloc in FileStorage.appendEntriesImpl -- allocates/frees buffer each call | `file_storage.zig:300-303` | Alloc cost (~50-200ns) dwarfed by fdatasync (~1-10ms); reduces heap fragmentation over millions of appends | [#197](https://gitlab.com/devnw/zig/raft/-/issues/197) | Retain reusable buffer on FileStorage struct | Resolved |
| 22 | P3 | 12 | sendHeartbeats builds full AE for caught-up peers | `node.zig:947-1021` | Short-circuit in buildAppendEntries when next > lastIndex; returns empty AE without log.slice | [#161](https://gitlab.com/devnw/zig/raft/-/issues/161) | Short-circuit for caught-up peers in buildAppendEntries | Resolved |
| 23 | P3 | 8 | make dev includes redundant check before test | `Makefile` | Removed `check` from `dev` target; `dev: fmt test-unit` | [#162](https://gitlab.com/devnw/zig/raft/-/issues/162) | Drop check from dev target | Resolved |
| 24 | Info | 4 | No performance metrics/counters exposed | `node.zig` | Stats struct with 8 counters: proposals, entries_applied, elections_started, became_leader, heartbeats_sent, snapshots_installed, ticks, messages_stepped | [#163](https://gitlab.com/devnw/zig/raft/-/issues/163) | Stats struct + perf_stats field + stats() accessor | Resolved |

---

## Findings by Area

### Hot-Path Allocations (PERF-001, 002, 003, 004, 005, 012)

**5 of 6 resolved.** Stack buffers eliminate heap allocations in commit path (PERF-001), dynamicPeers (PERF-004), sendHeartbeats parallel arrays (PERF-012). Double log.slice eliminated (PERF-002). Zero-copy borrowUnstable() added (PERF-003). Remaining deferred: entry data double-duplication (PERF-005) -- test-only impact, requires Storage vtable change.

### FileStorage I/O (PERF-007, 008, 011, 013, 025, 028)

**6 of 6 resolved.** Batched WAL writes (PERF-007), bulk WAL compaction via byte-copy (PERF-008), bulk entriesImpl reads (PERF-011), bulk WAL recovery into memory (PERF-013), FileStorage benchmarks added (PERF-025), reusable batch buffer in appendEntriesImpl (PERF-028).

### Pipeline Architecture (PERF-006, 009, 010)

**3 of 3 resolved.** Cached lastIndex (PERF-010), split advancePersist/advanceApply for pipelining (PERF-006), start-offset tracking for O(1) front-removal (PERF-009).

### Init Path (PERF-021)

**1 of 1 resolved.** Double loadSnapshot() eliminated -- snapshot loaded once and passed through (PERF-021).

### CI/Build (PERF-014, 015, 016, 019, 027)

**5 of 5 resolved.** Zig download cached across jobs (PERF-014), needs dependency removed (PERF-015), check removed from make dev (PERF-019), benchmark infrastructure (PERF-016), benchmark raft module now ReleaseFast (PERF-027).

---

## Pass 2 Findings

Pass 2 focused on benchmark infrastructure quality, init-path efficiency, and FileStorage allocation patterns. 8 findings were evaluated; 4 scored high enough to track (PERF-021, 025, 027, 028) and 4 were downgraded below threshold:

| ID | Finding | Score | Disposition |
|----|---------|-------|-------------|
| PERF-027 | Benchmark raft module in Debug mode | 80 | **Tracked** -- P1, misleading benchmark results |
| PERF-025 | No FileStorage benchmarks | 48 | **Tracked** -- P2, zero WAL regression detection |
| PERF-021 | Double loadSnapshot() on init | 24 | **Tracked** -- P3, double disk read on restart |
| PERF-028 | Per-call buffer alloc in appendEntriesImpl | 12 | **Tracked** -- P3, minor allocator churn |
| PERF-022 | dynamicPeers O(V*O) during joint consensus | 4 | Not tracked -- max N=7, infrequent |
| PERF-023 | syncReplicationPeers heap ArrayList | 4 | Not tracked -- infrequent, tiny allocation |
| PERF-024 | clearAndFree in commitFinalizeChange | 4 | Not tracked -- extremely rare code path |
| PERF-026 | rpc.Message by-value through vtable | 4 | Not tracked -- ~128 bytes, negligible cost |

---

## Roadmap

All phases complete. 23 findings resolved, 1 deferred.

### Quick Wins (< 1 day) -- Complete

| ID | Effort | Expected Impact | Status |
|----|--------|-----------------|--------|
| PERF-001 | Low | 3-5x reduction in commit-check latency | **Resolved** |
| PERF-004 | Low | Eliminate 1 alloc/free per heartbeat | **Resolved** |
| PERF-017 | Trivial | 1 less alloc/free per election | **Resolved** |
| PERF-019 | Trivial | ~2-3s faster dev iteration | **Resolved** |
| PERF-014 | Low | ~5 min faster CI pipelines | **Resolved** |
| PERF-027 | Low | Accurate benchmark numbers (5-20x correction) | **Resolved** |
| PERF-028 | Low | Eliminate per-call alloc in WAL append | **Resolved** |

### Medium Effort (1-5 days) -- Complete

| ID | Effort | Expected Impact | Status |
|----|--------|-----------------|--------|
| PERF-002 | Low-Med | Eliminate 1 log.slice + alloc per advance cycle | **Resolved** |
| PERF-007 | Medium | Reduce WAL append syscalls from 2N to 2 | **Resolved** |
| PERF-010 | Low | Eliminate frequent vtable calls | **Resolved** |
| PERF-013 | Low-Med | 2x faster WAL recovery | **Resolved** |
| PERF-015 | Low | Faster CI pipeline start | **Resolved** |
| PERF-021 | Low-Med | Eliminate double disk read + BLAKE3 on restart | **Resolved** |
| PERF-025 | Medium | FileStorage regression detection (3 new benchmarks) | **Resolved** |

### Strategic (> 5 days) -- Complete

| ID | Effort | Expected Impact | Status |
|----|--------|-----------------|--------|
| PERF-003 | Medium | Zero-copy log reads on hot path | **Resolved** |
| PERF-005 | High | Halve follower write-path allocations | **Deferred** -- test-only impact |
| PERF-006 | High | 30-50% lower commit latency with FileStorage | **Resolved** |
| PERF-009 | Medium | O(1) front-removal in log | **Resolved** |
| PERF-016 | Medium | Performance regression detection | **Resolved** |
