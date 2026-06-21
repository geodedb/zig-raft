# FIPS-140-3 Status — raft

**REQ-ID:** REQ-FIPS-EXEMPT-001 — non-security crypto/hash/RNG sites are
allow-listed (exempt) from the workspace FIPS-140-3 routing requirement per
`docs/superpowers/specs/2026-06-20-fips-pqc-design.md` §5.

**Mode:** This library performs only **non-security** cryptographic, hashing,
and random-number operations. It is **exempt** from the workspace FIPS-140-3
routing requirement (see `fips` package spec
`docs/superpowers/specs/2026-06-20-fips-pqc-design.md` §5). No security
primitive in this library produces, protects, or verifies a secret, key,
credential, authentication tag, or signature; therefore none is routed
through the `crypto_backend` seam and none links the OpenSSL FIPS provider.
When a consumer builds with `-Dfips=true`, this library is unaffected: it
adds no `-Dfips` flag and changes no behavior.

Per spec §5 behavior table, BLAKE3 / CRC32 are allowed in this library only as
**non-security integrity** primitives (corruption/truncation detection), never
as authentication or confidentiality. They are documented exempt below.

## Exempt call sites

| File:Line | Primitive | Use | Why non-security (exempt) |
|-----------|-----------|-----|---------------------------|
| `src/snapshot.zig:104,117,229,332` | BLAKE3 (`std.crypto.hash.Blake3`) | Snapshot-data integrity digest (build/verify) + test helper | Detects corruption/truncation of snapshot bytes in transit and on disk; not authentication, not a secret |
| `src/file_storage.zig:327,425` | BLAKE3 (`std.crypto.hash.Blake3`) | Snapshot-data integrity digest on disk (save/load) | On-disk corruption detection; not authentication |
| `src/file_storage.zig:177,533,560,609,628` | CRC32 (`std.hash.Crc32`) | Hard-state / snapshot-meta record integrity | Corruption detection on the on-disk format; not tampering protection |
| `src/election.zig:20,30` | `std.Random.DefaultPrng` | Randomized election-timeout jitter | Liveness mechanism to break leader-election ties; not a secret or key |

Line numbers are pre-annotation approximations; each annotation adds one line
above its site.

## zlint allow-list

Every site above carries an inline `// fips-exempt: non-security — <reason>`
annotation so the `zlint` FIPS rule treats it as allow-listed rather than a
violation. Adding a new `std.crypto` / `std.hash` / `std.Random` use to this
library REQUIRES adding both the inline annotation and a row in the table
above.
