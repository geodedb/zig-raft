# Requirements

Last updated: 2026-03-21

## Scope

This repository owns the reusable Raft consensus core extracted from Geode. It does not own Geode-specific QUIC transport, protobuf framing, transaction/schema commands, or persistence adapter wiring.

## Functional Requirements

| ID | Requirement | Geode evidence |
|---|---|---|
| RAFT-REQ-001 | The library SHALL expose portable node state, log entry, message, config, and node types. | `src/distributed/raft.zig:38`, `:45`, `:64`, `:146`, `:157` |
| RAFT-REQ-002 | The library SHALL expose the core Raft lifecycle: init, start, tick, message handling, proposals, elections, append-entries, snapshot handling, and commit advancement. | `src/distributed/raft.zig:188`, `:242`, `:254`, `:285`, `:297`, `:363`, `:550`, `:650`, `:716` |
| RAFT-REQ-003 | The library SHALL preserve cluster-level correctness through the Geode e2e scenarios. | `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` |
| RAFT-REQ-004 | The library SHALL NOT own Geode transaction/schema/snapshot-pointer commands in the core boundary. | `src/distributed/raft_consensus.zig:62`, `:87`, `:94` stay outside the core |
| RAFT-REQ-005 | The library SHALL NOT own Geode QUIC transport, protobuf framing, or persistent-storage integration in the core boundary. | `src/distributed/raft_consensus.zig:135` through `:211`, `:215` through `:367`, `:412` through `:457` |

## Packaging Requirements

| ID | Requirement | Geode evidence |
|---|---|---|
| RAFT-PKG-001 | Geode build-time protobuf schema wiring SHALL remain adapter-level integration unless explicitly re-homed later. | `build.zig:431`, `:432`, `:433`, `:434` |
| RAFT-PKG-002 | Alternate scaffold/2PC/sharding helpers SHALL not be conflated with the Raft core extraction. | `src/dist/scaffold.zig:15` through `:259` |

## Test Requirements

| ID | Requirement | Geode evidence |
|---|---|---|
| RAFT-TEST-001 | Preserve core source-level unit coverage. | `src/distributed/raft.zig:754`, `:768`, `:788` |
| RAFT-TEST-002 | Preserve Geode cluster/e2e behavior coverage. | `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` |
| RAFT-TEST-003 | Preserve the current variance benchmark reference, noting it is synthetic today. | `src/tools/run_variance_benches.zig:300`, `:301`, `:308`, `:312`, `:314`, `:370`, `:398`, `:465` |

## Documentation Requirements

- Document the split between Raft core and transport/storage adapters.
- Document that the current variance benchmark is synthetic rather than a full replication benchmark.
- Document what remains Geode-specific: transaction/schema commands and protobuf/QUIC framing.

## Migration Requirements

1. Keep the consensus state machine in this library.
2. Leave transport, protobuf framing, storage, and Geode transaction/schema glue outside the core package.
3. Continue validating the extracted core against the Geode e2e cluster tests.
