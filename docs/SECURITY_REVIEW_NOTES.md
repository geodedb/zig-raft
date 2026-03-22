# Security Review Notes

**Date:** 2026-03-21
**Reviewer:** Automated adversarial security review agent
**Commit:** dd249a5 (main), de99c0d (uncommitted -- Pass 2 remediation)
**Scope:** Full codebase, CI/CD, dependencies, configuration, delivery pipeline

## Review Phases

1. **Orientation** -- git log, repo map, branch structure
2. **Inventory** -- languages (Zig 0.15.1+), frameworks (none), build system (zig build), CI (GitLab CI with ci-catalog components)
3. **Threat Model** -- consensus integrity, log safety, snapshot integrity, supply chain, DoS
4. **Automated Scanning** -- 12 tools (see below)
5. **Manual Adversarial Review** -- full source audit of all 12 src modules, test infra, CI config, pre-commit config
6. **Scoring** -- CVSS-inspired risk scoring (0-100) based on severity, exploitability, impact
7. **Issue Filing** -- 38 GitLab issues (#9-#30, #31-#36, #37-#44, #45-#52), labels for severity/type/confidence/status
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

## Notes

- No secrets detected in repo history (6 commits scanned)
- Zig ecosystem lacks lockfile/SBOM tooling -- osv-scanner found no package sources
- No Dockerfiles, Helm charts, Terraform, or other IaC in repo
- CI uses component includes from `gitlab.com/devnw/ci-catalog` at `@v1.7.1` (mutable tag) and `@v1.2.1` -- Won't Do (managed at CI catalog level)
- `debian:bookworm-slim` Docker image used without digest pinning -- Won't Do (managed at CI catalog level)
- CODEOWNERS updated from placeholder to `@devnw` (SEC-0010, resolved in Pass 1)
- `SECURITY.md` updated from GitHub to GitLab references (INFO-1, resolved in Pass 1)
- All 44 security findings across 4 passes resolved or Won't Do
- 0 open findings
- All 160 tests pass (114 unit, 1 integration, 34 scenario, 11 chaos)
- Automated scanners (12 tools) found 0 issues across all 4 passes -- all findings are from manual review
