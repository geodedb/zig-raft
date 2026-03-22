# Security Review Notes

**Date:** 2026-03-22
**Reviewer:** Automated adversarial security review agent
**Commit:** 50d3600 (main)
**Scope:** Full codebase, CI/CD, dependencies, configuration, delivery pipeline

## Review Phases

1. **Orientation** -- git log, repo map, branch structure
2. **Inventory** -- languages (Zig 0.15.1+), frameworks (none), build system (zig build), CI (GitLab CI with ci-catalog components)
3. **Threat Model** -- consensus integrity, log safety, snapshot integrity, supply chain, DoS
4. **Automated Scanning** -- 12 tools (see below)
5. **Manual Adversarial Review** -- full source audit of all 12 src modules, test infra, CI config, pre-commit config
6. **Scoring** -- CVSS-inspired risk scoring (0-100) based on severity, exploitability, impact
7. **Issue Filing** -- 101 GitLab issues (#9-#30, #31-#36, #37-#44, #45-#52, #53-#72, #73-#76, #77-#78, #79-#81, #82-#105, #106-#109, #114-#125), labels for severity/type/confidence/status
8. **GAP_ANALYSIS.md** -- updated with full stack-ranked findings table

## Tool Versions

| Tool | Version | Status |
|------|---------|--------|
| gitleaks | 8.30.0 | Ran -- 0 findings |
| trufflehog | 3.92.5 | Ran -- 0 findings |
| osv-scanner | 2.3.2 | Ran -- no package sources detected |
| trivy (fs) | 0.69.0 | Ran -- 0 findings |
| trivy (config) | 0.69.0 | Ran -- 0 findings |
| grype | 0.107.0 | Ran on SBOM -- 0 matches |
| syft | 1.41.1 | SBOM generated |
| semgrep | 1.150.0 | Ran (81 rules, 44 files) -- 0 findings |
| checkov | 3.2.499 | Ran (gitlab_ci) -- 10 passed, 0 failed |
| shellcheck | (via Haskell) | No .sh files in repo |
| hadolint | 2.14.0 | No Dockerfiles in repo |
| bandit | 1.9.3 | N/A (no Python source) |

## Scan Output Locations

All scan outputs written to `/tmp/`:
- `/tmp/gitleaks.json`
- `/tmp/trufflehog.json`
- `/tmp/sbom.cdx.json`
- `/tmp/grype.json`
- `/tmp/trivy-fs.json`
- `/tmp/trivy-config.json`
- `/tmp/semgrep.json`
- `/tmp/checkov.json`
- `/tmp/osv.json`

## Threat Model

### Assets

1. **Consensus state** -- term, voted_for, committed entries (HardState + log)
2. **Log integrity** -- entries must be identical across committed nodes
3. **Snapshot integrity** -- at-rest and in-transit
4. **Cluster membership** -- voter set, joint consensus transitions
5. **Availability** -- leader election, heartbeat, replication

### Threat Actors

1. **Compromised peer** -- Byzantine node sending crafted messages
2. **Network attacker** -- MitM on transport (mitigated by transport auth requirement)
3. **Disk corruption** -- bit rot, bad sectors, filesystem bugs
4. **Resource exhaustion** -- large entries, large snapshots, many peers

### Attack Surfaces

1. `step()` -- all inbound RPC processing (validated per SEC-0001, SEC-0022)
2. `receiveChunk()` -- snapshot reception (validated per SEC-0002, SEC-0007, SEC-0023, SEC-0024)
3. `handleAppendEntries()` -- log replication (validated per SEC-0009)
4. `recoverWal()` -- WAL deserialization (validated per SEC-0004, SEC-0027)
5. `deserializeVoters()` -- config deserialization (validated per SEC-0005, SEC-0006, SEC-0011)
6. `Config` -- constructor input validation (validated per SEC-0017)
7. FileStorage at-rest files -- hard_state, WAL, snapshot.data, snapshot.meta (SEC-0031, SEC-0032, SEC-0036)

## Findings Summary

### Pass 1: Initial Security Review (SEC-0001..SEC-0011)

All 11 findings remediated in-place with code changes and tests. Key fixes:
- BLAKE3 replaced CRC32 for snapshot checksums (SEC-0002)
- Sender ID validation added to `step()` (SEC-0001)
- Silent `catch {}` replaced with `try` in `propose()` (SEC-0003)
- `@enumFromInt` replaced with `std.meta.intToEnum` (SEC-0004)
- Checked multiplication and `@intCast` bounds checks (SEC-0005, SEC-0006)
- `max_entry_size` config with validation (SEC-0009)
- Stale snapshot rejection (SEC-0007)
- Empty voter set rejection (SEC-0011)

### Pass 2: Adversarial Follow-up Review (SEC-0012..SEC-0028)

17 new findings identified through deeper manual review (4 parallel review agents + direct audit).
**All 17 findings remediated** (15 code fixes, 2 Won't Do). Key fixes:
- Stack buffer overflows in quorum/replication replaced with dynamic allocation (SEC-0012, SEC-0013)
- Use-after-free in `buildAppendEntries()` fixed by reordering alloc/copy/free (SEC-0014)
- `ready()` now propagates errors instead of `catch {}` (SEC-0015)
- Election timer uses `std.math.mul` for checked overflow (SEC-0016)
- `Config.validate()` added and called from `RaftNode.init()` (SEC-0017)
- Snapshot `ChecksumMismatch` now calls `clearPending()` to prevent leaks (SEC-0023)
- `max_snapshot_size` added to Config with enforcement in `receiveChunk()` (SEC-0024)
- `maybeAppend()` uses `std.math.add` for checked arithmetic (SEC-0025)
- `log.slice()` bounds-validates before indexing unstable entries (SEC-0026)
- WAL recovery caps `data_len` at 64 MiB (SEC-0027)
- WAL writes have rollback semantics on mid-loop failure (SEC-0028)
- `debug.assert` replaced with runtime error returns (SEC-0020)
- `proposeConfigChange()` validates `max_entry_size` (SEC-0021)
- Sender validation logs `std.log.warn` on mismatch (SEC-0022)
- CI tag pinning (SEC-0018) and pre-commit --unsafe (SEC-0019): Won't Do, handled externally

### Pass 3: Deep Adversarial Review (SEC-0029..SEC-0036)

8 new findings identified through exhaustive line-by-line audit of all 12 source modules, focusing on:
- Error handling completeness and rollback semantics
- Data-at-rest integrity gaps
- Protocol-level correctness (snapshot config transfer)
- Input validation completeness (peer ID dedup)

**All 8 findings filed as GitLab issues (#37-#44). All 8 remediated.** Key fixes:
- Pre-validate serialized config size BEFORE `proposeChange()` to prevent state mutation on rejection (SEC-0029)
- Added `config: ClusterConfig` to `InstallSnapshot` RPC; `PendingSnapshot` stores owned config copy; assembled snapshot carries correct voter set (SEC-0030)
- `readEntryAt()` now reads full record including CRC32 footer and verifies on every read (SEC-0031)
- BLAKE3 digest of snapshot data stored in `snapshot.meta`; verified on `loadSnapshot()` (SEC-0032)
- Added `DuplicatePeerId` and `SelfInPeers` checks to `Config.validate()` (SEC-0033)
- Changed `finalizeChange()` return type to `!?rpc.Entry`; propagates errors instead of swallowing (SEC-0034)
- Added `std.log.warn` with leader ID and error name in `handleInstallSnapshot()` catch block (SEC-0035)
- Extended `hard_state` to 20 bytes (CRC32 footer), `snapshot.meta` to 52 bytes (BLAKE3 + CRC32); backward-compatible legacy reads (SEC-0036)

### Pass 4: Exhaustive Adversarial Review (SEC-0037..SEC-0044)

8 new findings identified through full re-audit of all 14 source modules, CI/CD config, and build files. Automated scanners re-run (all clean). Focused on:
- File I/O error handling and errdefer consistency
- Documentation accuracy (file format sizes)
- Data persistence completeness (snapshot config not round-tripped via FileStorage)
- Algorithmic complexity (O(n) linear scan on sorted data)
- Crash safety (non-atomic multi-file snapshot writes)
- Supply chain security (CI toolchain integrity)
- Input validation bypass (snapshot config copied without validation)
- Protocol correctness (static `config.peers` vs dynamic `membership_state.voters`)

**All 8 findings filed as GitLab issues (#45-#52). All 8 remediated** (7 code fixes, 1 Won't Do). Key fixes:
- Added `errdefer` for `snapshot.meta.tmp` cleanup on write/sync failure (SEC-0037)
- Updated FileStorage doc comment to reflect actual 20-byte `hard_state` and variable-length `snapshot.meta` formats (SEC-0038)
- Extended `snapshot.meta` to V3 variable-length format with serialized `ClusterConfig`; round-trips config through `saveSnapshotImpl`/`loadSnapshotImpl`; `compactImpl` preserves cached config; backward-compatible with legacy 16-byte and V2 52-byte formats (SEC-0039)
- Replaced O(n) linear `findIndexPos()` with binary search on sorted `raft_index` array (SEC-0040)
- Made snapshot save atomic: write both `.tmp` files first, rename meta (commit point), then rename data; `init()` cleans up stale `.tmp` files; `loadSnapshotImpl` recovers crashed data renames (SEC-0041)
- CI Zig toolchain download hash verification: Won't Do, managed at CI catalog level same as SEC-0008 (SEC-0042)
- Added `validateSnapshotConfig()` in `receiveChunk()`: rejects empty voter sets, duplicate IDs, and zero IDs (SEC-0043)
- Added `dynamicPeers()` helper; `becomeLeader()`, `sendHeartbeats()`, `startElection()` now use `membership_state.voters` (dynamic) instead of static `config.peers`, including joint consensus outgoing voters (SEC-0044)
- Added `serializeConfig`/`deserializeConfig` public helpers to `MembershipState` for config persistence

### Pass 5: Deep Review of Pass 1-4 Remediation Code (SEC-0045..SEC-0064)

20 new findings identified through exhaustive adversarial review of the ~1400 lines of security remediation code from Passes 1-4, which constitutes new attack surface. 4 parallel review agents covering: file_storage.zig, node.zig, snapshot.zig+membership.zig, and log+replication+election+quorum+ready+storage+transport. All 8 automated scanners re-run (0 findings). Focus areas:
- State mutation ordering relative to fallible operations (OOM resilience)
- Memory leak paths on error (errdefer coverage)
- Integer overflow/underflow in arithmetic from untrusted input
- Missing input validation at deserialization boundaries
- Peer authentication gaps in RPC response handlers
- Raft protocol compliance (§9.6 pre-vote, joint consensus)

**All 20 findings filed as GitLab issues (#53-#72). All 20 remediated.** Key fixes:
- `finalizeChange` and `proposeChange` reordered: serialize BEFORE mutating state (SEC-0045, SEC-0046)
- Added `errdefer` in `proposeConfigChange`, `tick`, `receiveChunk`, `buildAppendEntries` for OOM leak prevention (SEC-0047, SEC-0048, SEC-0049, SEC-0059)
- `std.math.add` for checked overflow in `expected_offset`, V3 meta validation, `maybeUpdate`, `startElection` (SEC-0050, SEC-0054, SEC-0055, SEC-0062)
- `InMemoryStorage.termImpl/entriesImpl` upper-bound guard returns `Unavailable` (SEC-0051)
- `handleInstallSnapshotResponse` validates sender is known peer (SEC-0052)
- `applyConfig` validates deserialized voter sets: empty, zero IDs, duplicates (SEC-0053)
- `handleAppendEntriesResponse` validates peer BEFORE term-demotion check (SEC-0056)
- `readSnapshotMeta` capped to 1 MiB, `loadSnapshotImpl` propagates errors (SEC-0057, SEC-0064)
- `proposeConfigChange` validates new_peers for zero IDs and duplicates (SEC-0058)
- `deserializeVoters` rejects non-0x00/0x01 type bytes (SEC-0060)
- Leaders reject pre-votes to prevent disruptive re-elections (§9.6) (SEC-0061)
- `receiveChunk` checks `max_snapshot_size` BEFORE allocation (SEC-0063)

### Pass 6: Full Re-audit with Automated Scanners (SEC-0065..SEC-0068)

4 new findings identified through full re-audit of all source modules, CI/CD config, build files, and pre-commit configuration. All 12 automated scanners re-run (0 findings). Focused on crash safety, concurrent access, thread safety, and supply chain.

**4 findings filed as GitLab issues (#73-#76). 3 remediated, 1 Won't Do.** Key fixes:
- Added `syncDir()` helper using `openat(".", O_DIRECTORY)` + `fsync`; called after every `rename()` in `saveHardStateImpl`, `saveSnapshotImpl`, `compactImpl`, `writeSnapshotMeta`, and recovery path in `loadSnapshotImpl` (SEC-0065)
- Added exclusive advisory lock via `.lock` file acquired at `init()`, held until `deinit()`; second `init()` on same directory returns `error.WouldBlock`; two new tests added (SEC-0066)
- Added doc comments to `RaftNode.allocator` field and `RaftNode.init()` documenting thread-safety requirement when spice parallelism is active (SEC-0067)
- `autofix_prs: true` in `.pre-commit-config.yaml` -- Won't Do, pre-commit configuration handled via external controls (SEC-0068)

### Pass 7: Adversarial Re-audit of Pass 6 Remediation Code (SEC-0069..SEC-0070)

2 new findings identified through full re-audit of all 14 source modules with 4 parallel adversarial review agents. All 12 automated scanners re-run (0 findings). 8+ agent-reported findings verified against actual code and filtered as false positives. Focused on:
- Error-path resource management in new SEC-0065/SEC-0066 code
- Consistency of checked arithmetic hardening across codebase
- Vote recording validation (confirmed filtered by quorum check — not a vulnerability)

**2 findings filed as GitLab issues (#77-#78). Both remediated.** Key fixes:
- Added `errdefer` for heap-allocated ArrayLists and WAL file in `FileStorage.init()`; changed `meta.freeConfig()` to `defer` so it runs on both success and error paths (SEC-0069)
- Applied `std.math.add` for checked overflow at all 4 `+ 1` locations in `ReplicationState.init/resetPeers`, `buildAppendEntries`, and `RaftLog.firstUnstableIndex` (SEC-0070)

**False positives filtered:**
- "Unvalidated vote recording from unknown candidates" — `tallyVotes()` iterates only over `voters` slice, so non-voter votes are never counted in quorum. Not a vulnerability.
- "Advisory lock can be deleted while held" — Lock scope is operator-error prevention, not adversarial defense. Data directory permissions are consumer responsibility (documented).
- "Lock fd leak on error path" — `errdefer lock_file.close()` fires correctly before `fs` construction; not a double-close.
- "usize→u32 casts without bounds" — All WAL cast sites already have `maxInt(u32)` bounds checks (SEC-0005).
- Multiple CI/supply-chain findings — Already tracked as Won't Do (SEC-0008, SEC-0018, SEC-0042, SEC-0068).

### Pass 8: Adversarial Re-audit of All Source Modules (SEC-0071..SEC-0073)

3 new findings identified through full re-audit of all 14 source modules with 5 parallel adversarial review agents. All 12 automated scanners re-run (0 findings). Focused on:
- Consistency of checked arithmetic hardening across node.zig (SEC-0070 follow-up)
- Memory lifecycle of snapshot data loaded at init
- Protocol correctness of config change propagation to followers and across restarts

**3 findings filed as GitLab issues (#79-#81). All 3 remediated.** Key fixes:
- Applied `std.math.add` for checked overflow at all 6 `+ 1` locations in `node.zig`: `propose()`, `proposeConfigChange()`, `tick()`, `ready()` (x2), `becomeLeader()` (SEC-0071)
- Added `freeSnapshot()` helper to `storage.zig`; `InMemoryStorage.loadSnapshotImpl` returns heap copies; `RaftLog.init` frees snapshot after extracting metadata; `RaftNode.init` uses snapshot config for MembershipState then frees (SEC-0072)
- `RaftNode.init()` restores MembershipState from snapshot config; `advance()` scans committed config entries and calls `applyConfig()`; `handleInstallSnapshot()` applies snapshot config on receipt (SEC-0073)

**False positives filtered (5 agents, 12+ agent-reported findings verified against actual code):**
- "Cross-set voter overlap in joint consensus config" — In Raft §6, a node CAN legitimately be in both old and new voter sets during joint consensus. Not a validation gap.
- "Missing errdefer for owned_voters_outgoing in receiveChunk" — After `toOwnedSlice()`, next operation is struct literal construction (infallible). No fallible operation between assignment and return.
- "Unvalidated vote recording from unknown candidates" — `tallyVotes()` iterates only over `voters` slice, so non-voter votes are never counted in quorum. Not a vulnerability. (Same as Pass 7.)
- "Missing snapshot config cross-check in handleInstallSnapshot" — `validateSnapshotConfig()` validates config; cross-checking against expected membership is impossible since snapshots carry new configs by design.
- "Pre-vote recursive call error handling" — Error propagates via `try`; election timeout retries on next tick. Not a security vulnerability.
- "Snapshot config not re-validated on intermediate chunks" — BLAKE3 integrity check prevents MITM tampering of any chunk data.
- "Bounds check missing in InMemoryStorage.compactImpl" — Only called from `RaftLog.compact()` which validates `compact_index <= committed <= lastIndex()`. Test-only storage.
- "Integer overflow in appendEntriesImpl/stableTo/compact/maybeCommit (5 locations)" — All require u64::MAX or usize::MAX values (>18 exabytes in memory). Physically impossible.
- "Incomplete gitleaks allowlist for zig-cache" — gitleaks only scans git-tracked content; both cache directories are in `.gitignore`.
- "Race condition in `ready()`" — Raft core is single-threaded by design; `spice` parallelism is read-only.
- Multiple CI/supply-chain findings — Already tracked as Won't Do (SEC-0008, SEC-0018, SEC-0042, SEC-0068).

### Pass 9: Full Codebase-Wide Adversarial Review (SEC-0074..SEC-0097)

24 new findings identified through exhaustive adversarial review with 6 parallel agents covering all source modules, tests, CI/CD, and configs. All 12 automated scanners re-run (0 findings). Focus areas:
- Protocol correctness: snapshot response handling, config change replication, HardState restoration
- Memory safety: uninitialized frees, leak-on-mid-loop-failure, borrowed slice lifetimes
- Receiver-side validation: entry count/size limits, vote recording, conflict_index validation
- Log/snapshot state management: log.restore() dead code, compact ordering, snapshot data caps
- Test infrastructure: leak detection blind spots, invariant checker completeness

**24 findings filed as GitLab issues (#82-#105). All open.** Key findings:
- `handleInstallSnapshotResponse` ignores `msg.success`, never updates peer progress — infinite snapshot loop (SEC-0074)
- `replication_state.peers` never updated after config changes — new voters unreachable, minority commit possible (SEC-0075)
- `maybeDecrTo` allows forward jump of `next_index` — malicious follower can halt replication (SEC-0076)
- `RaftNode.init()` does not restore HardState — double-voting on restart (SEC-0077)
- `handleInstallSnapshot` never calls `log.restore()` — log state diverges from snapshot (SEC-0090)
- `FileStorage.compactImpl` defer frees uninitialized entries — heap corruption on mid-loop failure (SEC-0078)
- `advance()` silently swallows config apply errors — membership permanently stale (SEC-0079)
- WAL rollback does not truncate orphaned bytes — ghost entries on crash recovery (SEC-0081)
- Checker only verifies terms, not entry data — can mask Raft safety violations (SEC-0089)

**Scanner results (Pass 9):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings |
| checkov | 3.2.499 | 10 passed, 0 failed |
| shellcheck | latest | No shell scripts |
| hadolint | 2.14.0 | No Dockerfiles |
| snyk | N/A (not installed) | Skipped |

### Pass 10: Full Codebase-Wide Adversarial Review (SEC-0098..SEC-0101)

4 new findings identified through exhaustive adversarial review with 5 parallel agents covering all 14 source modules, all test files, CI/CD config, build files, and pre-commit config. All 12 automated scanners re-run (0 findings). 30+ agent-reported findings verified against actual code; 26+ filtered as false positives or duplicates of existing findings (Passes 1-9).

**4 findings filed as GitLab issues (#106-#109). All resolved.** Key findings:
- `receiveChunk()` accepts `last_included_term=0` — added term=0 validation + unit test (SEC-0098, resolved)
- `handleInstallSnapshot()` bypasses validation — added `applySnapshotConfig()` in `membership.zig` with full validation (SEC-0099, resolved)
- `Config.validate()` minimum chunk size raised to 1024 bytes + unit test (SEC-0100, resolved)
- Added `tests/scenarios/adversarial.zig` with 12 test cases exercising SEC-0001..SEC-0100 validations (SEC-0101, resolved)

**Scanner results (Pass 10):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (81 rules, 44 files) |
| checkov | 3.2.499 | 0 failed |
| shellcheck | latest | No shell scripts |
| hadolint | 2.14.0 | No Dockerfiles |
| snyk | N/A (not installed) | Skipped |

**False positives filtered (26+):**
- "Out-of-bounds slice in maybeAppend" — offset = loop index `i`, always `< entries.len` by construction
- "Leader commit term not validated in followers" — correct Raft §5.3 behavior
- "Double-voting race in startElection" — Ready-State pattern; consumer persists hard state
- "Unchecked leader_commit bounds" — clamped by `commitTo(@min(committed, lastIndex()))`
- "match_index not bounded to leader's log" — `commitTo()` clamping prevents commit beyond log
- "Log compaction below committed index" — `appliedTo()` enforces `applied <= committed`
- "Cross-set voter overlap in joint consensus" — valid per Raft §6 (same as Pass 8)
- "CI Zig download SHA256" — duplicate of SEC-0008/SEC-0042 (Won't Do)
- "Transport auth not enforced" — design decision documented in `transport.zig:9-18`
- "CRC32 for persistent state" — documented design tradeoff (Passes 2-3)
- "File permissions not set" — consumer responsibility per docstring
- "Symlink attack on data dir" — consumer responsibility per docstring
- "Election timeout overflow" — `std.math.mul` with `catch maxInt(u32)` is safe
- "Empty voter set in vote handler" — prevented by `Config.validate()` and `MembershipState.init()`
- "Unchanged config in proposeChange" — not a security issue
- "TOCTOU snapshot rename" — mitigated by exclusive file lock (SEC-0066) + crash recovery

### Pass 11: Deep Manual Adversarial Review (SEC-0103..SEC-0106)

4 new findings identified through deep manual adversarial review with 5 parallel agents. Focus on error-path state consistency, integer overflow in accumulation patterns, and initialization ordering. All 12 automated scanners re-run (0 findings). 5+ agent-reported findings verified and filtered as false positives.

**4 findings filed as GitLab issues (#110-#113). All resolved.** Key findings:
- `becomeLeader()` deferred role assignment until after all fallible operations (SEC-0103, resolved)
- `buildEntries()` `total_bytes` now uses `std.math.add` checked addition (SEC-0104, resolved)
- `FileStorage.init()` added `errdefer` rollback between sequential `appendSlice` calls (SEC-0105, resolved)
- `tickElection()`/`tickHeartbeat()` now use saturating addition `+|` + unit test (SEC-0106, resolved)

**Scanner results (Pass 11):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (81 rules, 44 files) |
| checkov | 3.2.499 | 0 failed |
| shellcheck | latest | No shell scripts |
| hadolint | 2.14.0 | No Dockerfiles |
| snyk | N/A (not installed) | Skipped |

**False positives filtered (5+):**
- "WAL wal_end u64 overflow" — requires ~18 exabytes of WAL data, not reachable
- "WAL recovery offset + total_record_size overflow" — bounded by actual file_size
- "Snapshot @intCast(offset) truncation" — usize-to-u64 is identity or widening cast
- "Snapshot chunk_size unvalidated" — already fixed by SEC-0100 (minimum 1024)
- "appendSlice double-free via meta.voters_buf" — ArrayList.appendSlice copies data, no aliasing

### Pass 12: Deep Manual Adversarial Review (SEC-0107..SEC-0108)

2 new findings identified through deep manual adversarial review with 5 parallel agents. Focus on memory safety in error paths and resource exhaustion via unbounded inputs. 7 automated scanners re-run (0 findings). 8+ agent-reported findings verified and filtered as false positives.

**2 findings filed as GitLab issues (#114-#115). All resolved.** Key findings:
- `receiveChunk()` use-after-free: freed pending snapshot not nulled before fallible `validateSnapshotConfig()` (SEC-0107, CRITICAL, resolved)
- `validateSnapshotConfig()` unbounded voter count: no limit on voter set size enables O(n²) CPU DoS and unbounded memory alloc (SEC-0108, LOW, resolved)

**Scanner results (Pass 12):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (81 rules, 44 files) |

**False positives filtered (8+):**
- "Quorum `committedForSet` off-by-one at `buf[n/2]`" — math correct for all cluster sizes
- "log.zig `slice()` bounds wraparound" — properly bounded by checks at lines 104, 107, 121
- "InMemoryStorage `entriesImpl` missing index validation" — line 159 catches OOB
- "Snapshot rename race" — BLAKE3 mismatch detection + recovery path handles crash scenarios
- "Ready duplicate message emission" — by design; Raft is idempotent
- "Missing errdefer for `owned_voters_outgoing`" — no fallible ops follow
- "`truncateFrom` missing fsync" — caller batches sync after full batch
- "Cross-set voter deduplication" — valid during joint consensus (Raft §6)

### Pass 13: Full Re-audit Adversarial Review (SEC-0109..SEC-0110)

2 new findings identified through exhaustive adversarial review with 5 parallel agents covering all 14 source modules, tests, CI/CD, and configs. Focus on state mutation ordering (SEC-0084 pattern) and Raft §6 joint consensus invariants. 7 automated scanners re-run (0 findings). 6 agent-reported findings verified and filtered as false positives.

**2 findings filed as GitLab issues (#116-#117). All resolved.** Key findings:
- `FileStorage.compactImpl()` mutates `snapshot_term`/`snapshot_index` before fallible `writeSnapshotMeta()` — same SEC-0084 pattern missed in FileStorage (SEC-0109, MEDIUM, resolved)
- `validateSnapshotConfig()` and `applySnapshotConfig()` accept `is_joint=true` with empty `voters_outgoing` — inconsistent joint consensus state (SEC-0110, LOW, resolved)

**Scanner results (Pass 13):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (81 rules, 44 files) |

**False positives filtered (6):**
- "Missing leader validation in AppendEntries" — Raft protocol uses term-based acceptance, not membership verification
- "Race condition in ReplicationState" — spice parallelism is read-only per design
- "Unbounded slice in `truncateUnstable()`" — `keep` clamped by construction
- "Integer overflow in trim_count" — `first_unstable >= 1` prevents underflow
- "Off-by-one in max_bytes entry count" — intentional (matches etcd/raft: always include at least one entry)
- "Snapshot term validation gap" — `node.zig` rejects stale terms before reaching `SnapshotState`

### Pass 14: Deep Adversarial Review (SEC-0111..SEC-0118)

8 new findings identified through exhaustive adversarial review with 5 parallel agents. Focus on state mutation ordering, error-path resource management, protocol correctness, and vtable consistency. 7 automated scanners re-run (0 findings). 20+ agent-reported findings verified and filtered as false positives or Won't Do duplicates.

**8 findings filed as GitLab issues (#118-#125). All resolved.** Key findings:
- `applyConfig()` missing is_joint/voters_outgoing consistency check — SEC-0110 gap (SEC-0111, MEDIUM)
- `pending_config_change`/`pending_config_index` not reset on snapshot application — blocks future config changes (SEC-0112, MEDIUM)
- `handleInstallSnapshotResponse` uses stale `snapshot_state.snapshot_index` — can inflate match_index (SEC-0113, MEDIUM)
- `saveSnapshotImpl` config corruption on OOM after clearing cached arrays (SEC-0114, MEDIUM)
- `appendEntriesImpl` missing rollback on `file.sync()` failure — state inconsistency (SEC-0115, MEDIUM)
- `finishLoadSnapshot` missing errdefer for config buffers on try error paths (SEC-0116, LOW)
- `ready()` missing errdefer on `log.slice()` result when `addCommittedEntries` OOMs (SEC-0117, LOW)
- `InMemoryStorage.entriesImpl` panics on inverted range — `lo == hi` should be `lo >= hi` (SEC-0118, LOW)

**Scanner results (Pass 14):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (81 rules, 44 files) |
| checkov | 3.2.499 | 10 passed, 0 failed |

**False positives filtered (20+):**
- "handleAppendEntries calls becomeFollower before entry validation" — correct Raft behavior per Figure 2
- "Shallow @memcpy in log.slice() creates dangling data pointers" — by design, same as etcd/raft
- "committed_entries in Ready are borrowed slices" — consumer loop protocol documented
- "ready/advance TOCTOU on committed entries" — consumer loop is sequential, no interleaved mutations
- "buildAppendEntries entries leaked after sendMsg" — ownership follows Ready lifecycle
- "CI Zig download without checksum" — duplicate of SEC-0008/SEC-0042 Won't Do
- "check-yaml --unsafe" — duplicate of SEC-0019 Won't Do
- "CODEOWNERS granularity" — hardening suggestion, not vulnerability
- "leader_commit trusted" — clamped by commitTo, transport integrity is consumer responsibility
- "Deserialization trailing bytes" — unreachable, entry data length exact
- "BLAKE3 does not cover config" — config validated independently
- "maybeDecrTo ignores conflict_term" — design choice, not vulnerability
- "compact() fallback term" — defensive saturation behind valid range guard
- "Election timeout overflow" — consumer config responsibility

### Pass 15: Deep Adversarial Review (SEC-0119..SEC-0121)

3 new findings identified through exhaustive adversarial review with 5 parallel agents. Focus on protocol-level safety properties (Election Safety, joint consensus liveness), response handler correctness, and CI config scanning coverage. 8 automated scanners re-run (0 findings). 30+ agent-reported findings verified; 27+ filtered as false positives or Won't Do duplicates.

**3 findings filed as GitLab issues (#126-#128). All resolved.** Key findings:
- Response handlers accept stale-term votes/acks — Election Safety violation via phantom votes (SEC-0119, CRITICAL)
- `pending_config_index` not tracked on follower config application — joint consensus deadlock on leader failover (SEC-0120, CRITICAL)
- Gitleaks allowlist unanchored regex suppresses `.gitlab-ci.yml` scanning (SEC-0121, LOW)

**Scanner results (Pass 15):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| trivy (config) | 0.69.0 | 0 misconfigs |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (p/default config) |
| checkov | 3.2.499 | 10 passed, 0 failed |

**False positives filtered (27+):**
- "Shallow @memcpy creates dangling data pointers" — by design, consumer loop protocol (duplicate of Pass 14)
- "Use-after-free in ReadyState after advance()" — by design, sequential consumer loop (duplicate of Pass 14)
- "buildAppendEntries shallow copy after free" — ownership follows Ready lifecycle (duplicate of Pass 14)
- "Missing errdefer for new_outgoing in saveSnapshotImpl" — false positive: appendSlice is last fallible op, empty ArrayList on failure
- "Snapshot chunk restart race" — by design, offset=0 starts fresh transfer
- "pending_config_change flag inconsistency" — by design, set from parsed.is_joint
- "Final config doesn't validate prior joint" — log consistency guarantees order
- "Pre-commit autofix" — duplicate of SEC-0068 Won't Do
- "PGP key not in SECURITY.md" — documentation preference
- "No branch protection in CI" — project-level GitLab setting
- "No CI job timeouts" — project-level GitLab setting
- "Pre-commit shell injection" — hardcoded pattern, no vector
- "InProcessTransport unbounded queue" — test harness, not production
- "Vtable null pointer risk" — standard Zig pattern, not a bug
- "WAL offset integer overflow" — theoretical, requires u64::MAX offset
- "restore() ownership" — standard Zig borrowing, caller retains
- "Underflow in buildAppendEntries" — guard at line 109 prevents this
- "Missing leader failover test during joint" — test gap, not vulnerability

### Pass 16: Deep Adversarial Review (SEC-0122..SEC-0136)

15 new findings identified through exhaustive adversarial review with 5 parallel agents covering protocol safety, storage/crash safety, transport/RPC validation, CI/CD/supply chain, and memory safety/API misuse. 8 automated scanners re-run (0 findings). 40+ agent-reported findings verified against code; 25+ filtered as false positives or duplicates.

**15 findings filed as GitLab issues (#129-#143). All resolved.** Key findings:
- `handleInstallSnapshot` leaks snapshot data/config on `applySnapshotConfig()` failure (SEC-0122, HIGH)
- Joint-consensus snapshot doesn't set `pending_config_index` — extends SEC-0120 to snapshot path (SEC-0123, HIGH)
- `RaftLog.append()` leaks `data_copy` on OOM + no batch rollback (SEC-0124, MEDIUM)
- `InMemoryStorage.appendEntriesImpl` same leak + no rollback (SEC-0125, MEDIUM)
- `RaftNode.init()` missing errdefers for `log_inst`/`membership_inst` (SEC-0126, MEDIUM)
- `max_bytes_per_msg` check bypassed when `max_entry_size == 0` (SEC-0127, MEDIUM)
- Entry index consistency not validated in `handleAppendEntries` (SEC-0128, MEDIUM)
- CI `ZIG_VERSION` injectable via pipeline trigger API (SEC-0129, MEDIUM)
- `init()` deletes snapshot `.tmp` files before crash recovery (SEC-0130, MEDIUM)
- `ready_state.prev_hard_state` not restored from persisted state (SEC-0131, LOW)
- Missing `syncDir()` after WAL deletion in compaction (SEC-0132, LOW)
- Pre-vote lacks leader lease check (SEC-0133, LOW)
- RPC request handlers don't validate sender membership (SEC-0134, LOW)
- RPC handlers accept `leader_id/candidate_id == 0` (SEC-0135, LOW)
- No `workflow:rules` restricting CI pipeline execution (SEC-0136, LOW)

**Scanner results (Pass 16):**

| Tool | Version | Result |
|------|---------|--------|
| gitleaks | 8.30.0 | 0 findings |
| trufflehog | 3.92.5 | 0 findings |
| osv-scanner | 2.3.2 | No package sources |
| trivy (fs) | 0.69.0 | 0 vulns, 0 secrets |
| syft + grype | 1.41.1 / 0.107.0 | 0 vulns |
| semgrep | 1.150.0 | 0 findings (p/default config) |
| checkov | 3.2.499 | 0 failed |
| shellcheck | 0.10.0+ | No shell scripts in repo |

**False positives filtered (25+):** See GAP_ANALYSIS.md Pass 16 section for full list.

## Notes

- No secrets detected in repo history (13 commits scanned)
- Zig ecosystem lacks lockfile/SBOM tooling -- osv-scanner found no package sources
- No Dockerfiles, Helm charts, Terraform, or other IaC in repo
- CI uses component includes from `gitlab.com/devnw/ci-catalog` at `@v1.7.1` (mutable tag) and `@v1.2.1` -- Won't Do (managed at CI catalog level)
- `debian:bookworm-slim` Docker image used without digest pinning -- Won't Do (managed at CI catalog level)
- CODEOWNERS updated from placeholder to `@devnw` (SEC-0010, resolved in Pass 1)
- `SECURITY.md` updated from GitHub to GitLab references (INFO-1, resolved in Pass 1)
- 135 total security findings across 16 passes: all 135 resolved or Won't Do
- Pass 16 remediation: SEC-0122 errdefer snapshot leak, SEC-0123 pending_config_index from snapshot, SEC-0124 log.append errdefer+rollback, SEC-0125 storage.append errdefer+rollback, SEC-0126 init errdefers, SEC-0127 max_bytes_per_msg outside entry_size guard, SEC-0128 entry index validation, SEC-0129 ZIG_VERSION sanitization, SEC-0130 .tmp deletion after recovery, SEC-0131 restored hard state in ready_state, SEC-0132 syncDir after WAL deletion, SEC-0133 pre-vote leader lease check, SEC-0134 membership check on request handlers, SEC-0135 reject ID==0, SEC-0136 workflow:rules
- Pass 15 remediation: SEC-0119 stale-term rejection, SEC-0120 pending_config_index in advance(), SEC-0121 gitleaks regex anchoring
- Pass 14 remediation: SEC-0111..SEC-0118 (8 findings)
- Pass 12 remediation: SEC-0107 null pending snapshot before fallible call, SEC-0108 voter count limit (MAX_VOTERS_PER_SET=255)
- Pass 11 remediation: SEC-0103 deferred role assignment, SEC-0104 checked addition, SEC-0105 errdefer rollback, SEC-0106 saturating tick counters
- Pass 10 remediation: SEC-0098 term=0 validation, SEC-0099 centralized `applySnapshotConfig()`, SEC-0100 minimum 1024-byte chunk size, SEC-0101 adversarial test suite (12 scenarios)
- Pass 9 remediation: 21 findings resolved in code, 3 closed as Won't Do (SEC-0093 harness allocator intentional, SEC-0099 caller responsibility, SEC-0102 test-only)
- All 185 tests pass (124 unit, 1 integration, 49 scenario, 11 chaos) after Pass 15 remediation
- Automated scanners (8 tools) found 0 issues across all 16 passes -- all findings are from manual adversarial review
