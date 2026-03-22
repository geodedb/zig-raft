# Performance Gap Analysis

**Last updated:** 2026-03-22
**Scope:** Full codebase performance review — algorithmic hot paths, memory allocations, I/O patterns, parallelism, CI/CD pipeline, missing benchmarks.
**Methodology:** 4 parallel review agents (algorithmic hot paths, memory allocations, I/O + parallelism, build + CI) analyzing all 14 source modules, tests, build system, and CI configuration.

## Summary

| PerfSeverity | Count | Description |
|--------------|-------|-------------|
| P0 | 1 | SLO/hot-path risk — heap allocations + quadratic loop in commit path |
| P1 | 5 | Material user-facing — double allocations, uncached peer lists, sequential pipeline |
| P2 | 10 | Moderate — I/O batching, vtable overhead, CI efficiency, missing benchmarks |
| P3 | 3 | Minor — clearAndFree, redundant AE build, make dev |
| Info | 1 | Hardening — missing performance metrics |

**20 findings total (1 resolved, 19 open).** Top 5 risks:
1. Commit path does 4-6 heap alloc/free per check + O(V*N) quadratic gather (PERF-001)
2. Same log slice fetched twice per ready/advance cycle (PERF-002)
3. log.slice() always allocates even for in-memory unstable reads (PERF-003)
4. Entry data duplicated twice on follower write path (PERF-005)
5. advance() couples persist+send sequentially (PERF-006)

## Baseline Measurements

| Test Suite | Duration | MaxRSS | Tests |
|------------|----------|--------|-------|
| Unit tests | 16ms (run) | 2M | 124 |
| Integration | 918us (run) | 2M | 1 |
| Scenarios | 28ms (max run) | 3M | 49 |
| Chaos | 222ms (run) | 12M | 11 |
| **Total compile+run** | **~0.6s** | **38M (compile)** | **185** |

No benchmarks exist (PERF-016). All measurements are test-suite timing only.

---

## Stack-Ranked Findings

| Rank | Sev | Score | Finding | Component | Evidence | Issue | Fix Summary | Status |
|------|-----|-------|---------|-----------|----------|-------|-------------|--------|
| 1 | P0 | 100 | Heap allocs + O(V*N) in commit path — 4-6 alloc/free per maybeCommit + quadratic gather in committedForSet | `replication.zig:188-216`, `quorum.zig:34-59` | 2 allocs in maybeCommit, 1 in committedForSet, O(V*N) nested loop; called per AE response + per leader tick | [#144](https://gitlab.com/devnw/zig/raft/-/issues/144) | Stack buffers [16]u64, pass HashMap directly | Open |
| 2 | P1 | 80 | Double log.slice() in ready/advance — same committed entries fetched twice with allocation | `node.zig:392-408,437-441` | log.slice allocates + copies entry metadata; identical range requested in ready() then advance() | [#145](https://gitlab.com/devnw/zig/raft/-/issues/145) | Scan pending_committed_entries in advance() | Open |
| 3 | P1 | 80 | log.slice() always allocates for unstable-only reads — copies contiguous in-memory data | `log.zig:103-113` | alloc + @memcpy even when range is entirely in unstable_entries; called per-peer per-heartbeat | [#146](https://gitlab.com/devnw/zig/raft/-/issues/146) | Return borrowed view for unstable-only reads | Open |
| 4 | P1 | 60 | dynamicPeers() allocates per heartbeat — fresh []u64 on every call | `node.zig:194-239` | alloc + free per sendHeartbeats call (every heartbeat_tick); O(V^2) dedup in joint consensus | [#147](https://gitlab.com/devnw/zig/raft/-/issues/147) | Cache peer list, invalidate on membership change | Open |
| 5 | P1 | 60 | Entry data duplicated twice on follower path — log.append dupe + storage dupe | `log.zig:49-63`, `storage.zig:136-148` | allocator.dupe in log.append + allocator.dupe in InMemoryStorage.appendEntriesImpl for same data | [#148](https://gitlab.com/devnw/zig/raft/-/issues/148) | Ownership transfer from unstable to storage | Open |
| 6 | P1 | 48 | advance() forces sequential persist+send — no pipelining possible | `node.zig:419-477` | Consumer cannot parallelize persist and send; adds fdatasync latency (1-10ms) sequentially | [#149](https://gitlab.com/devnw/zig/raft/-/issues/149) | Split into advancePersist/advanceApply or document send-before-advance | Open |
| 7 | P2 | 48 | Per-entry WAL writes — 2N syscalls instead of 1 batched write | `file_storage.zig:277-348` | writeAll(payload) + writeAll(crc) per entry in loop; per-entry alloc/free for serialize buffer | [#150](https://gitlab.com/devnw/zig/raft/-/issues/150) | Single buffer + single writeAll for entire batch | Open |
| 8 | P2 | 48 | WAL compaction reads/writes one-at-a-time — ~2N syscalls | `file_storage.zig:599-681` | readEntryAt per entry (seek+read+CRC); then write per entry to new WAL | [#151](https://gitlab.com/devnw/zig/raft/-/issues/151) | Bulk read/write of contiguous byte range | Open |
| 9 | P2 | 36 | O(n) shift in stableTo/compact — copyForwards on every advance | `log.zig:206-222,238-258` | O(remaining) memory copy to shift entries forward in ArrayList | [#152](https://gitlab.com/devnw/zig/raft/-/issues/152) | Ring buffer or start-offset tracking | Open |
| 10 | P2 | 36 | lastIndex() vtable call when unstable empty — frequent on followers | `log.zig:65-69` | Falls through to storage.lastIndex() vtable dispatch; called from commitTo, isUpToDate, ready, advance, propose | [#153](https://gitlab.com/devnw/zig/raft/-/issues/153) | Cache last_index field on RaftLog | Open |
| 11 | P2 | 36 | FileStorage.entriesImpl per-entry seeks — N seek+read instead of 1 bulk read | `file_storage.zig:351-379` | readEntryAt per entry with individual seekTo; contiguous WAL could be bulk-read | [#154](https://gitlab.com/devnw/zig/raft/-/issues/154) | Bulk readAll of contiguous byte range | Open |
| 12 | P2 | 32 | sendHeartbeats parallel arrays heap-allocated per tick | `node.zig:958-961` | futures + results arrays allocated/freed every heartbeat in parallel mode | [#155](https://gitlab.com/devnw/zig/raft/-/issues/155) | Stack-allocated [16]T buffers | Open |
| 13 | P2 | 32 | WAL recovery reads each record twice (header then full) | `file_storage.zig:857-927` | 21-byte header read then seek back + full read per record; 2N reads instead of 1 bulk | [#156](https://gitlab.com/devnw/zig/raft/-/issues/156) | Bulk readAll of entire WAL into memory | Open |
| 14 | P2 | 32 | CI downloads Zig 7 times per pipeline | `.gitlab-ci.yml` | ~45MB download + apt-get install in each of 7 jobs | [#157](https://gitlab.com/devnw/zig/raft/-/issues/157) | Pre-built image or GitLab cache | Open |
| 15 | P2 | 24 | CI test jobs blocked on build without using artifacts | `.gitlab-ci.yml` | needs: [build] creates serial bottleneck; tests recompile from source | [#158](https://gitlab.com/devnw/zig/raft/-/issues/158) | Remove needs or add .zig-cache sharing | Open |
| 16 | P2 | 24 | No benchmarks exist — zero regression detection | build.zig, Makefile | No bench targets, no ns/op measurements, no CI perf gates | [#159](https://gitlab.com/devnw/zig/raft/-/issues/159) | Create benchmarks/ with core hot-path benchmarks | Open |
| 17 | P3 | 16 | resetVotes uses clearAndFree instead of clearRetainingCapacity | `election.zig:119` | Frees + reallocates HashMap backing on every election start | [#160](https://gitlab.com/devnw/zig/raft/-/issues/160) | One-line change to clearRetainingCapacity | Resolved |
| 18 | P3 | 12 | sendHeartbeats builds full AE for caught-up peers | `node.zig:947-1021` | buildAppendEntries runs log.term + log.slice even when peer is caught up (empty result) | [#161](https://gitlab.com/devnw/zig/raft/-/issues/161) | Short-circuit with emptyHeartbeat for caught-up peers | Open |
| 19 | P3 | 8 | make dev includes redundant check before test | `Makefile` | check compiles code that test-unit recompiles; ~2-3s wasted per iteration | [#162](https://gitlab.com/devnw/zig/raft/-/issues/162) | Drop check from dev target | Open |
| 20 | Info | 4 | No performance metrics/counters exposed | `node.zig` | Consumers cannot observe proposals_applied, replication_lag, election counts | [#163](https://gitlab.com/devnw/zig/raft/-/issues/163) | Add Stats struct with atomic counters | Open |

---

## Findings by Area

### Hot-Path Allocations (PERF-001, 002, 003, 004, 005, 012)

The library makes excessive heap allocations on the critical path. The commit check alone (PERF-001) does 4-6 alloc/free per invocation. Combined with double-slicing (PERF-002), always-allocating slice (PERF-003), per-heartbeat peer list (PERF-004), and double entry copy (PERF-005), a leader processing 10K entries/sec could be doing 100K+ unnecessary allocations/second.

**Quick wins:** Stack buffers in maybeCommit/committedForSet (PERF-001), clearRetainingCapacity (PERF-017), cache dynamicPeers (PERF-004).

### FileStorage I/O (PERF-007, 008, 011, 013)

FileStorage uses per-entry I/O syscalls for writes, reads, compaction, and recovery. Batching these into bulk operations would significantly reduce syscall overhead, especially at high throughput.

**Quick wins:** Batched WAL writes (PERF-007), bulk WAL recovery (PERF-013).

### Pipeline Architecture (PERF-006, 009, 010)

The sequential persist→apply pipeline in advance() and the O(n) entry shifting create unnecessary latency. Caching lastIndex and using a ring buffer would reduce per-cycle overhead.

**Strategic:** Split advance() for persist/send pipelining (PERF-006), ring buffer for unstable_entries (PERF-009).

### CI/Build (PERF-014, 015, 016, 019)

CI wastes ~5-10 minutes per pipeline on redundant Zig downloads and serial job dependencies. Missing benchmarks prevent performance regression detection.

**Quick wins:** Cache Zig download (PERF-014), remove build dependency from tests (PERF-015), drop check from make dev (PERF-019).

---

## Quick Wins (Low effort, immediate impact)

| ID | Effort | Expected Impact |
|----|--------|-----------------|
| PERF-001 | Low | 3-5x reduction in commit-check latency |
| PERF-004 | Low | Eliminate 1 alloc/free per heartbeat |
| PERF-017 | Trivial | 1 less alloc/free per election |
| PERF-019 | Trivial | ~2-3s faster dev iteration |
| PERF-014 | Low | ~5 min faster CI pipelines |

## Medium Effort (Meaningful improvement)

| ID | Effort | Expected Impact |
|----|--------|-----------------|
| PERF-002 | Low-Med | Eliminate 1 log.slice + alloc per advance cycle |
| PERF-007 | Medium | Reduce WAL append syscalls from 2N to 2 |
| PERF-010 | Low | Eliminate frequent vtable calls |
| PERF-013 | Low-Med | 2x faster WAL recovery |
| PERF-015 | Low | Faster CI pipeline start |

## Strategic (Design-level improvements)

| ID | Effort | Expected Impact |
|----|--------|-----------------|
| PERF-003 | Medium | Zero-copy log reads on hot path |
| PERF-005 | High | Halve follower write-path allocations |
| PERF-006 | High | 30-50% lower commit latency with FileStorage |
| PERF-009 | Medium | O(1) front-removal in log |
| PERF-016 | Medium | Performance regression detection |
