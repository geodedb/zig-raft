# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.15] - 2026-09-12

### Changed

- Converge thread_pool v0.2.7, fips v0.1.1, protobuf v0.1.5, config v0.1.13 (GAP-0838 diamond sync).

## [0.1.14] - 2026-09-12

### Changed

- Converge thread_pool v0.2.7, fips v0.1.1, protobuf v0.1.5 (GAP-0838 diamond sync).

## [0.1.9]

### Fixed

- **Candidate liveness backstop, part 2 (RC-B it.2): timer-based candidate
  demotion + backoff.** v0.1.8's `VoteLost` demotion only fires when a *quorum
  rejects* the candidate. A transport-degraded / isolated candidate — or a
  pre-vote *mutual-grant* lockstep where peers grant each other's pre-votes so no
  rejection quorum ever forms — collects neither a winning nor a rejecting
  quorum and, without a timer, stays a candidate indefinitely (the residual half
  of the geode k8s no-leader frozen-term state, observed on `v0.6.27-30`). Now,
  after `Config.candidate_demote_threshold` (default 4) consecutive failed
  candidate elections, the node steps down to follower and applies a growing,
  capped, randomized election backoff (`ElectionState.applyElectionBackoff`),
  desynchronizing lockstep peers so one campaigns ahead and wins. Liveness-only —
  no term/log change, no safety property affected; `candidate_demote_threshold=0`
  restores the prior re-elect-forever behaviour. New `Stats.candidate_demotions`.
  - Regression scenario `stuck candidate demotes to follower after consecutive
    failed elections` (RED before the fix: an isolated pre-vote candidate never
    demotes) + `applyElectionBackoff` unit test in `election.zig`.

## [0.1.8]

### Fixed

- **Candidate liveness backstop (RC-B): etcd-style demotion on quorum vote
  rejection.** `handleRequestVoteResponse` previously detected only a vote WIN
  (`hasJointQuorum`); a candidate denied by a quorum stayed in the election loop
  until its election timer fired, so a node frozen behind the cluster under
  pre-vote (or a split-vote loser) never stepped down to grant a competitor's
  vote. Now, mirroring etcd's `case quorum.VoteLost: r.becomeFollower(...)`, a
  candidate (pre-vote or real) rejected by a quorum demotes to follower at the
  current term and resets its randomized election timer. Consensus-layer half of
  the geode k8s no-leader all-candidate livelock (geode
  `docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md`, RC-B;
  closes `ISSUE-candidate-liveness-backstop` Option A). Liveness-only —
  `becomeFollower` keeps the current term, so no safety property changes.
  - New `ElectionState.hasRejectQuorum` / `hasJointRejectQuorum` (loss-side dual
    of `hasQuorum` / `hasJointQuorum`), unit-tested in `election.zig`.
  - Regression scenarios `candidate_demotes_on_quorum_prevote_rejection` and
    `candidate_demotes_on_quorum_realvote_rejection` in
    `tests/scenarios/election.zig`.

## [0.1.0] - YYYY-MM-DD

### Added

- Initial release
- Basic functionality
- Documentation and examples

[Unreleased]: https://gitlab.com/devnw/zig/raft/-/compare/v0.1.0...HEAD
[0.1.0]: https://gitlab.com/devnw/zig/raft/-/releases/v0.1.0
