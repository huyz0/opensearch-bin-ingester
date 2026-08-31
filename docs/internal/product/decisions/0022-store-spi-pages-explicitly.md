# 0022. The store SPI pages explicitly, and `ObjectStat` carries no timestamp

Status: accepted
Date: 2026-09-01
Requirements: FR-8, NFR-2, and cost rules R2, R9
Supersedes: two decisions in [docs/research/30-design-space/07-pluggable-store-abstraction.md](../../../research/30-design-space/07-pluggable-store-abstraction.md) §1

## Context

Doc 07 §1 sketched the store SPI a year before any code, with

```java
Stream<ObjectStat> list(String prefix, String startAfter);   // lazily paged
record ObjectStat(String key, long size, Version version, long lastModifiedMillis) {}
```

Both are wrong for this system, and both are expensive to change once four
backends implement them — hence an ADR rather than a comment.

## Decision

### 1. `ListPage list(prefix, startAfter, maxKeys)`, not `Stream`

⚠️ **A lazy `Stream` makes LIST requests invisible to the meter.** Paging lives
inside the backend's spliterator, so the counting decorator (R9) observes ONE
invocation whether S3 issued one request or ten thousand. The headline
acceptance criterion of M1 is `store.totalRequests() == 0` across 1,600 idle
consumers; with a stream, that assertion can pass over thousands of billed
requests. A cost meter that cannot see the expensive operation is the one
defect this project exists to prevent.

Two consequences follow that a stream could not deliver:

- **The error contract holds.** Every I/O method on `BinStore` declares
  `IOException` so a caller can distinguish "lost the CAS race" (an empty
  `Optional`) from "the store is unreachable". A failure after the first page of
  a stream can only surface as an unchecked exception from a terminal
  operation — precisely what the checked exception exists to prevent.
- **A governor can refuse.** FR-21's LIST ceiling is a *refusal*, and refusing
  page 500 of a stream means throwing from inside a terminal op.

⚠️ This costs nothing in requests: S3 `ListObjectsV2`, GCS and Azure all return
size and etag/generation per entry, so `ObjectStat` is filled from the listing
and no backend needs a `stat` per key.

### 2. `ObjectStat` carries no `lastModifiedMillis`

Object stores disagree on the precision of last-modified and on whether it
changes under a copy. **No decision in this system may depend on one**: discovery
is by key and by the commit log, never by timestamp ordering (ADR-0002). A field
present in the record is a field somebody will sort by, and a backend deriving a
`Version` from mtime produces one that changes on every read — which the
conformance suite now refuses outright.

⚠️ If a later milestone genuinely needs it, adding a record component changes the
canonical constructor. That is why `CostTable` is in `Capabilities` from the
first version even though M1.3 is the first reader.

## Alternatives considered

**Keep `Stream` and count inside each backend.** Rejected: the count would live
in eight implementations instead of one decorator, and R9's whole point is that
the meter is a decorator no backend can forget to call. It also leaves the
error-contract and refusal problems untouched.

**Keep `Stream` and have the decorator count `Spliterator.tryAdvance`.** Rejected:
that counts *entries*, not requests, and the ratio between them is the page size
the backend chose. It is a plausible-looking meter that reports the wrong number,
which is worse than no meter.

**Keep `lastModifiedMillis` but document that nothing may sort by it.** Rejected:
a field present in a record is a field somebody sorts by, and the failure is
silent and data-dependent. The conformance suite already refuses a `Version`
derived from mtime; carrying the timestamp invites exactly that backend.

**Add `lastModifiedMillis` later if needed.** Rejected as the DEFAULT but kept as
the escape hatch: adding a record component changes the canonical constructor,
so it is a signature change across every backend rather than an addition. That
asymmetry is why `CostTable` ships in `Capabilities` now, before its first
reader.

## Consequences

- Doc 07 §1 carries a revision banner pointing here; §3's withdrawn bullet carries an inline note rather than a section banner.
- Doc 07 §3's conformance bullet "pages lazily without buffering all keys" is
  **withdrawn**: it demanded the property this ADR removes. It is replaced by
  "one call is one page is one request", asserted by
  `listPagesAtMaxKeysAndResumesWhereItStopped`.
- Backends must not fan out internally: one `list` call is one provider request.
