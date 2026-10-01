# 0083. The fast journal reaches its disk through one JournalFile seam

Status: accepted
Date: 2026-10-01
Requirements: FR-17
Research: docs/research/30-design-space/12-fast-mode-wal-and-quorum.md

## Context

ADR-0081 has every pod hold fast entries in an append-only journal on its
`emptyDir`, and ADR-0082 §4 fixes the journal's records, its torn-tail rule
and its compaction by rename. That is the project's first business logic that
needs a local file. Non-negotiable 7 says business logic touches no socket,
clock or object store directly, and the gate bans `java.nio.file` and
`java.nio.channels` everywhere but `binstore-backends`; code-structure.md
rule 4 says the seams are few and named, and a new one is an ADR.

## Decision

- **One seam, `JournalFile`, in `binstore-spi`**: `readAll`, `append`,
  `force`, a durable `truncate`, an atomic and durable `replace`, `size` and
  `close`. Appended is not durable until `force` returns, and a crash may keep
  any prefix of the unforced bytes -- the contract the journal is written
  against, stated on the interface.
- **The journal's logic, `FastJournal`, is in `sequencer`** and holds no file:
  recovery (decode, refuse a whole record it cannot read, replay releases and
  drops, then rewrite the file atomically to the held entries before the first
  append -- which cuts a torn tail and, after a failed fsync, never trusts
  bytes a page cache may hold that the disk does not), the first I/O failure
  ending the journal, the held entries, the byte cap as a refusal the caller
  turns into backpressure, release, drop, and compaction once released bytes
  pass half the file.
- **The record codec, `FastJournalRecord`, is in `format`** beside the other
  formats, sharing the segment's record encoding.
- **Two implementations in `binstore-backends`**, the module exempt from the
  I/O gate: `FileJournalFile` (a `FileChannel`; `replace` writes a sibling,
  fsyncs it, renames it over the journal atomically and fsyncs the directory)
  and `MemoryJournalFile`, the fake, which loses unforced bytes or keeps a torn
  prefix of them on an injected crash. Both are held to
  `JournalFileConformance` (in `binstore-spi`'s test fixtures).
- `binstore-spi` is the object-store seam's module; it holds this one because
  it is the dependency-free module the exempt adapter module already
  implements against. Placing the adapter anywhere else would mean widening
  the I/O gate's exemption list -- weakening a gate, which non-negotiable 2
  forbids for thresholds and the same reasoning forbids here.

## Alternatives considered

- **`java.nio` in `FastJournal` with the file named as an exemption**, as
  `ConfigFile` is. Rejected: it widens a gate's exemption list, and the
  journal's crash behaviour -- the thing its tests must drive -- cannot be
  injected into a real file.
- **A generic "local storage" seam** (files by name, directories). Rejected:
  one append-only file with seven operations is all ADR-0082 needs, and a wider
  seam is a wider surface for the gate to stop policing.
- **The journal on the object store.** Rejected by ADR-0013 and ADR-0081: the
  journal exists to answer before any object-store request.

## Consequences

- The seams are now six: `BinStore`, `Clock`, `Sequencer`,
  `SubscriptionTransport`, `Membership`, `JournalFile`.
- The epoch file ADR-0082 §4 also places on the `emptyDir` is not part of
  this seam's first use; it lands with the epoch fence (M13.26c) and either
  shares this seam or is decided there. ⚠️ Decided by M13.26c: it SHARES the
  seam, a second `JournalFile` written only by `replace` -- the whole-file
  write beside, fsync, rename, directory fsync that ADR-0082 §4 asks of it --
  so no seventh seam exists.
- Windows cannot fsync a directory; `FileJournalFile` treats that as best
  effort there and as an error elsewhere. The pods run on Linux.
