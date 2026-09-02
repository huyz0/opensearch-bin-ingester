# 0025. Reserve the lane byte in the segment directory entry now; wire it later

Status: accepted
Date: 2026-09-02
Requirements: FR-18
Research: docs/research/30-design-space/01-object-layout-and-format.md

## Context

[ADR-0014](0014-priority-lanes.md) decided lane must be a first-class field in
both the record's wire frame and the segment's directory entry ("adding it
later is a format change"), and `roadmap.md`'s own milestone table promises
"the `i8 lane` field lands in the wire format at M3". Investigating what that
promise actually requires (M3 spec-drafting, M2.17) found it ties, per
ADR-0014/0015, to a materially larger routing-resolution feature — a
producer-supplied routing value, murmur3-hashed to a partition matching
OpenSearch's own `OperationRouting`, and the plugin pushing index metadata to
the ingester over its subscription channel. That is several times the size of
M3's own completion condition (the adaptive flush interval, NFR-6) and was
confirmed out of scope for this milestone before drafting M3's SPEC.

The narrower question this ADR actually answers: given the full lane
FEATURE is deferred, should the BYTE itself still be reserved now, in M3, or
deferred alongside it to M10?

## Decision

**Reserve the byte now; wire it later.** `format.SegmentFormat`'s directory
entry gains a trailing `i8 lane` field, widening from 48 to 49 bytes.
`SegmentWriter` always encodes `0` (no caller anywhere in the tree has a real
lane value to pass — the concept does not exist above this layer yet).
`SegmentReader` decodes the same byte into a new `RunEntry.lane()` field,
observed as `0` by every M3-produced segment.

**This is a version bump, not a silent width change.** Per the
`wire-format-change` skill's own first checklist item ("is a version bump
enough... a reader must handle the old shape until every possible writer of
it has aged out"): `SegmentFormat.VERSION` moves from 0 to 1.
`SegmentReader` accepts BOTH — `VERSION_0` (48-byte entries, no lane byte,
what every segment written before this ADR looks like) and `VERSION` (49
bytes, the reserved byte) — computing the directory entry width from the
segment's own preamble rather than assuming one. A version-0 entry has no
lane byte to read; the reader synthesises `0` for it rather than leaving the
field's meaning depend on which version was actually parsed. `SegmentWriter`
only ever emits `VERSION` going forward — no code path writes `VERSION_0`
anymore, matching every other "readers lag writers by at least one release"
case this project's format changes already follow.

**No caller above `format` changes.** `RunKey` gains no lane component (a
producer cannot supply one, so there is nothing to group runs by yet); no
change to `BulkParser`'s `_bulk` action-line parsing or the in-process
`RecordStream` contract. Only the on-disk shape moves.

## Alternatives considered

- **Defer the byte alongside the feature, to M10.** Rejected on ADR-0014's own
  stated reasoning, which this ADR does not reopen: "adding it later is a
  format change" — M10 would then need its own version bump AND a second
  golden-file pair, for one byte that costs nothing to reserve now. The
  reservation is cheap (one field, ~10 lines across writer/reader, one new
  golden fixture) precisely because M3 is already the milestone doing the
  version-bump mechanics for other reasons (the adaptive interval touches no
  wire format, but this ADR's own change does) — paying the mechanical cost
  once, not twice, is the entire point of ADR-0014's forward-reference.
- **Repurpose one of the record-level flags byte's 5 currently-unused bits**
  instead of widening the directory entry. Rejected: ADR-0014 places lane in
  "the segment RUN ENTRY" BY NAME (not the per-record flags byte), and the
  directory entry is where every other structural, non-payload property of a
  run already lives (`recordCount`, `minTimestampMillis`, `codecFlags`).
  ⚠️ **This reserves the location ADR-0014 names, without deciding the
  question that location implies.** ADR-0014 is explicit that lane is
  genuinely per record ("it must be, to be useful"); this ADR does not
  reopen that. What M10 still has to design is HOW a per-record value ends
  up representable at the run level — most plausibly by grouping runs on
  `(index, partition, lane)` rather than just `(index, partition)`, so every
  record inside one run legitimately shares the same lane and the directory
  entry's byte is that shared value, not a second, competing per-record
  field. This ADR reserves the byte at the location ADR-0014 names; it is
  silent on that grouping question on purpose, because M3's own scope
  (`milestones/M3/SPEC.md`'s "Out — deliberately" section, confirmed with
  the person running that session before drafting) explicitly defers all of
  ADR-0014's scheduling design to M10, not just its implementation. The
  record flags byte is a different structure serving a different purpose
  (op type, version-presence) and conflating the two would make a future
  reader hunt across two unrelated byte layouts for "where lane lives."
- **Widen the entry without a version bump**, accepting that pre-M3 segments
  become unreadable. Rejected outright: this project has not shipped, so no
  real segment exists yet that this would strand — but the discipline is
  worth building correctly before it matters, not after. `GoldenSegmentTest`
  exists precisely to catch exactly this class of break, and its own
  docstring ("do NOT regenerate this file to make the test pass") states the
  principle this alternative would have violated.
- **Round the entry up to a 4-byte-aligned 52 bytes**, reserving 3 extra spare
  bytes for whatever comes after lane. Rejected: no other field is known to
  need reserving, and speculative padding is exactly the kind of assumption
  this project's own "measure, don't assume" discipline argues against —
  three unexplained zero bytes with no named purpose is worse than a 49-byte
  entry with one named, documented one. If a second field turns up before
  M10, it pays the same one-time version-bump cost this one just did.

## Consequences

- One byte per run entry, negligible against the 8 MiB segment-size target
  and the existing 48-byte entry (≈2% larger directory, itself a small
  fraction of segment size) — no measurable cost-model impact (rule 6:
  request rates scale with segments, never with bytes per entry).
- `format.SegmentFormat.directoryEntryBytesFor(int version)` is now the
  single source of truth for "how wide is a directory entry", replacing the
  bare `DIRECTORY_ENTRY_BYTES` constant everywhere except the one place
  (`SegmentWriter`, which only ever emits the current version) that may still
  use it directly.
- Two golden fixtures now exist side by side: `golden/segment-v0.bin`
  (unmodified, still asserting the old 48-byte shape a real earlier build
  produced) and the new `golden/segment-v1.bin` (asserting the new 49-byte
  shape, lane always `0`). Neither is regenerated to make the other's test
  pass.
- M10 inherits a byte that already round-trips, already has a name, and
  already has a reader that will decode whatever real value M10 starts
  writing — it inherits zero of the version-bump mechanics this ADR pays for.
- `RunEntry.lane()` is `0` for every segment this codebase can currently
  produce or has ever produced under `VERSION_0`. No behavior anywhere
  branches on it yet; it is inert until M10 gives it a meaning.
