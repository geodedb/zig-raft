# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
