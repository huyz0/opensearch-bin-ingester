# 0053. A forwarded commit is a format type, not a JSON body

Status: accepted
Date: 2026-09-18
Requirements: FR-11, FR-12
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

M8.20 lands the production `SequencerTransport` — the pod-to-pod forwarding hop
that M5.6e has owed since M5, and without which every append to a
non-leaseholder pod fails. It has to put a `CommitRequest`
`(podId, incarnationId, flushSeq, segmentKey, recordCounts)` on a wire and read
a `CommitDelta` back.

`CommitDelta` already has a format, with golden files. The request does not:
M4 shaped it as a record of values "so that the record shape does not have to
change when the remote implementation lands", and the remote implementation is
now landing.

The obvious move is for the transport to serialise it — a JSON body, or a few
`writeUTF` calls — because it is small, internal, and goes one hop between two
pods of the same deployment.

## Decision

**A forwarded commit is a format type in `format`, with a magic, a version, a
golden file and a decoder that refuses rather than guesses** —
`CommitRequestFrame`, magic `BPCR`, v1. The transport carries bytes and owns no
encoding of its own.

The rules it inherits from every other type in that package, and why each one
matters on this particular path:

- **An unknown version stops; it does not skip.** A reader that took the part it
  understood would apply a commit whose shape nobody sent.
- **`!=` rather than `>`** on the version, because a version *below* the known
  one is what zeroed torn bytes carry.
- **The stream count is bounded by the bytes remaining.** ⚠️ An earlier draft
  of this bullet said the guard prevents an allocation, and review measured that
  false — the map is built with no capacity, so nothing is sized from the count.
  What it buys is that a torn frame is refused there, with a message an operator
  can act on, rather than twenty streams later as a truncation.
- **A stream id is two big-endian longs, not a string.** That is how every other
  `RunKey` in the package goes on a wire, it is 16 bytes rather than 37, and
  `UUID.fromString` is lenient enough that `0-0-4000-8000-1` and the canonical
  form are two byte sequences decoding to one id — which contradicts the
  determinism this format promises.
- **The frame enforces every guard `CommitRequest` does, including the podId
  grammar.** It is what arrives from another process, so a guard it omits
  becomes an unchecked throw during the conversion, in the loop serving that
  peer. ⚠️ Round 2 of this commit's review found exactly one such omission.
- **Every narrowing read is bounded and every construction is inside the
  `IOException` boundary.** Review measured the shape that motivates this: with
  the bound removed, a varint partition of `0x80000000` narrowed to a negative
  int and `RunKey`'s own guard threw an `IllegalArgumentException` straight out
  of `decode`, past its `throws IOException` — the unchecked throw that kills
  the loop serving a peer.
- **Trailing bytes are a refusal**, not slack.
- **The record counts are sorted before they are written**, so one commit is one
  sequence of bytes.

## Alternatives considered

- **JSON, or the transport writing fields itself.** Rejected on what the fields
  ARE rather than on taste. `incarnationId` exists because `(podId, flushSeq)`
  cannot tell a restart from a replay (ADR-0036): `flushSeq` restarts at 0 on
  every process start while `podId` is stable. A loose encoding is one where
  that field is the easiest to drop as noise — it looks like a debugging aid —
  and dropping it makes a restart indistinguishable from a replay on the path
  where the answer decides whether records are committed twice or not at all.
  The same argument applies to a record count: a JSON object that loses one
  stream has the leaseholder assign a range too short, and every record past it
  becomes unreachable with no error anywhere.
- **Reusing `CommitDelta` in both directions.** Rejected: a delta carries an
  assigned `sequence` and per-run offsets, which the requester does not know and
  must not invent. A request-shaped delta would have the sender fill in zeros
  for the fields the leaseholder is supposed to decide, and a bug that used them
  would be invisible.
- **A gRPC or protobuf schema.** Rejected for the reason this project has no
  protobuf anywhere: it is a dependency and a code generator in the commit path,
  for one message, in a tree that already has a hand-written framing convention
  with golden files and a `Cursor` that bounds every read. The cost is not the
  bytes, it is a second way to describe a format.
- **No golden file, round-trip tests only.** Rejected, and this one is worth
  stating because it is the most tempting: encode-then-decode passes for any
  self-consistent pair of methods, including one that swaps `flushSeq` and a
  record count. The writer and the reader move together while every request in
  flight between two pods running different builds commits the wrong number of
  records under the wrong flush. ⚠️ **AND THIS FORMAT IS THE ONE WHERE A ROLLING
  DEPLOY GUARANTEES MIXED VERSIONS**: unlike a segment, which one build writes
  and another reads later, a forwarded commit is sent by the pod being replaced
  to the pod replacing it, at exactly the moment their builds differ.

## Consequences

- **A format change here is a `wire-format-change` commit** — the format, every
  reader, every writer, the fakes, the golden files and this ADR together.
- **The transport becomes thin**, which is what lets M8.20 be about sockets and
  status codes rather than about fields. Its failure modes (a refusal, an
  ambiguous timeout) are the interesting part and are all that is left in it.
- **`CommitRequest` and `CommitRequestFrame` are two types with the same
  fields**, which is a real cost and is deliberate: `CommitRequest` is
  `sequencer`'s in-process argument and `format` may not depend on `sequencer`.
  The conversion is one method each way and lives in the transport. ⚠️ **The risk
  is that they drift**, and nothing mechanical prevents it — if a field is added
  to one and not the other, the compiler is silent because neither references
  the other. A `check-wire-parity` predicate over the two records' components
  would close it; it is recorded as a backlog row rather than claimed here.
- **Two golden files, not one**: the two-stream fixture and the smallest legal
  request, because the zero `flushSeq` and the single-entry count are where an
  off-by-one in the varints shows and the larger fixture hides it.
