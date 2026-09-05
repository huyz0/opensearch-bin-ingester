# 0032. A batched delta carries many segments, under the reserved kind

Status: accepted
Date: 2026-09-05
Requirements: NFR-1, FR-4
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

M4's cost model rests on one number: **commits per second, not commits per
stream**. The commit chain is written by exactly one writer at
`1 / commitBatchInterval`, which the SPEC's own table prices at $52/month for a
250 ms interval and $259/month at 50 ms. Scope item 5 requires that one delta
carry *every stream and every contributing pod flush* in its window, and
acceptance criterion 9 asserts the **pod** dimension specifically, because the
stream dimension already holds and asserting only it would constrain nothing.

Today it cannot hold. `CommitDelta(long sequence, String segmentKey,
List<RunCommit> runs)` names exactly **one** segment. Every pod flush produces
its own segment, so K pods flushing inside one window need K segment keys in one
chain entry — and there is nowhere to put them. With the current shape the only
way to commit K flushes is K entries, so the commit rate scales with pods, which
non-negotiable 6 forbids by name.

⚠️ This is the constraint that decides the shape, and it is not the batching
logic: no scheduler, interval or queue can make a one-segment record carry two
segments. The format has to change first, which is why this ADR precedes the
batcher rather than accompanying it.

## Decision

**A delta carries a list of segments, each with its own runs.**

```
CommitDelta(long sequence, List<SegmentCommit> segments)
SegmentCommit(String segmentKey, List<RunCommit> runs)
```

**The new layout claims `KIND_DELTA = 0` under the existing v1 kinded header.**
That kind was left unassigned deliberately by ADR-0028's implementation, whose
comment says so: *"Kind 0 is deliberately UNASSIGNED … It is left free so a
FUTURE delta layout can claim it, at which point v0 and it are genuinely
different shapes."* This is that future layout, and the condition the comment
set — genuinely different shapes — is met.

**Encoding is canonical and version-selecting:**

- exactly one segment → **v0**, byte-for-byte what is written today;
- two or more → **v1 kinded, kind 0**.

**Decoding accepts both**, and a v0 delta decodes to a one-element segment list.

⚠️ **A single-segment v1 delta is decodable but never written.** That
asymmetry is deliberate and is the price of keeping round-trips byte-stable:
`decode(v1-with-one-segment)` yields a delta whose canonical `encode()` is v0,
so that one input does not round-trip to its own bytes. Making it round-trip
would mean always writing v1, which churns every byte in the bucket and both
golden files to say what v0 already says — precisely what ADR-0028's
implementation refused. Round-trip stability is therefore defined over
**canonical** encodings, and the golden files pin both directions.

## Alternatives considered

**Put the segment key on each run** — `RunCommit(key, recordCount, firstOffset,
segmentKey)`, delta becomes `(sequence, List<RunCommit>)`. Rejected on bytes and
on truth. Segment keys are long: `…/<index>/<partition>/<epoch>/<seq>.bseg` runs
to ~80 bytes, and a saturated 250 ms window across 6 pods and 1,000 streams
would repeat one of six keys across thousands of runs — tens of kilobytes of
duplication in an object written four times a second. It also encodes a
falsehood the reader must then defend against: nothing in the type stops two
runs of the same segment naming different keys.

**One delta per segment, written at a bounded rate** — keep the shape, let the
batcher emit K entries per window. Rejected: it *is* the defect. K entries per
window makes the commit rate scale with pods, so criterion 9 fails by
construction and the $52/month becomes $52 × pods.

**A new top-level version (v2) rather than the reserved kind.** Rejected as
waste: the kinded header exists to carry exactly this, kind 0 was reserved for
exactly this, and burning a version number would leave the reservation unused
and unexplained.

**Always encode v1, single segment included.** Rejected — see the canonical-
encoding note above. It buys byte-stable round-trips for one never-written input
and pays for it by rewriting every delta in every bucket and both golden files.

## Consequences

**Makes easy:** the batcher M4.7 needs, and with it the pod dimension of
criterion 9. One PUT per window regardless of how many pods flushed into it.

**Makes hard — and this is the real cost:** `SubscriptionHub` builds a `Push`
from `delta.segmentKey()`, one per run. With many segments per delta it must
iterate segments and pair each run with *its* segment, and getting that pairing
wrong sends a consumer to the wrong object — a silent data error, not a crash.
That reader is updated in this same commit, as non-negotiable 8 requires.

**Forecloses:** nothing. Kind 0 was reserved and is now spent; kinds 1 and 2
(SEAL, CONTINUE) are untouched, and v0 stays readable for as long as any bucket
holds it.

**Cost:** unchanged per delta. A one-segment delta is byte-identical to today's.
A multi-segment delta adds one uvarint for the segment count plus one
length-prefixed key per additional segment — and it exists only where the
alternative was a whole additional object, so it strictly reduces both PUTs and
bytes. No request count moves: recovery still reads one object per chain entry,
and there are now fewer entries.
