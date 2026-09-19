# 0058. A partitioned pod acks on a durable commit intent

Status: accepted
Date: 2026-09-20
Requirements: FR-4, FR-11, NFR-3, NFR-8
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

FR-4 says a write is acknowledged only once its segment is durable AND its
offset is committed. A pod that can reach the store but not its sequencer --
an AZ partition -- can do the first and not the second, so until now every
flush it made failed and its producers retried into the partition.

Research 03 section 10 names the degraded path: write a commit-intent object
into `ctl/inbox/<slot>/` with `putIfAbsent`, and have the leader drain it, so
"a network partition delays visibility rather than stopping writes". M8.14 is
that path, and it cannot be built without deciding what the producer is told
while the offset does not yet exist.

## Decision

**During a partition, a 202 means the segment AND a putIfAbsent'd commit
intent are durable; the offset is assigned when the leaseholder drains the
inbox.** The user chose this on 2026-09-20 over refusing writes until the
partition heals and over deferring the inbox.

- The intent is `<prefix>/ctl/inbox/0/<podId>/<incarnationId>/<flushSeq>.intent`,
  its body the `CommitRequestFrame` a forward would have carried. One slot
  (S = 1), named in the key so a second slot is a directory, not a format.
- **One PUT per pod per slot per flush**, never per record or index (cost.md
  rule 6). A retried flush finds its key occupied and defers again.
- Any forward failure qualifies, an ambiguous one included: the drain applies
  an intent through the per-pod dedupe window (ADR-0036), so a flush whose
  forward had in fact landed is answered, never committed twice.
- A store that refuses the intent fails the flush: nothing durable to ack on,
  and criterion 12 says those acks stop.
- The append completes with `AppendResult.deferred`: no offset, and nothing
  pushed to subscribers, because a record with no offset is indexed nowhere.
- **A pod that has deferred forwards nothing** until the leaseholder confirms a
  drain, and as leader drains before committing locally: the dedupe window
  keeps one high mark per incarnation, so an intent overtaken by a later flush
  would be refused as a replay -- an acked write lost.
- The drain runs when a deferring pod reaches the leaseholder again and once
  per takeover, never on a LIST timer. Drains on one term are serialised, so
  one intent never lands twice in a batch, and the cost at heal is the
  intents plus one LIST per asking pod. Each pod's intents apply in order; a
  pod whose intent sticks keeps deferring, and no other pod is held by it.
- The orphan sweep keeps every segment an intent names, reading the inbox and
  then the chain again, since a drain writes the delta before it deletes the
  intent.

## Consequences

- RPO stays 0: the records are in the segment and the commit that will make
  them visible is in the store.
- Visibility waits for the drain. A pod that wrote intents and then died
  before any heal leaves them for the next takeover's drain.
- FR-4's wording is amended to name this case; a 202 no longer always implies
  an assigned offset.
