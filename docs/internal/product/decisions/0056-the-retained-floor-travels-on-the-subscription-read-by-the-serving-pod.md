# 0056. The retained floor travels on the subscription, read by the serving pod on demand

Status: accepted
Date: 2026-09-19
Requirements: FR-9, FR-10, NFR-2, NFR-13
Research: docs/research/30-design-space/09-consumer-position-and-watermarks.md, docs/research/30-design-space/03-metadata-and-cas.md

## Context

Since M7.10 the checkpoint's `oldestRetainedOffset` is true: garbage
collection reports what it actually deleted and the checkpoint carries it.
Since M7.16 the consumer can refuse a position below it
(`ConsumerClient.retainedFrom`, then `PositionCollectedException`). Nothing
connects the two (M7.18): the consumer holds no checkpoint reader, by design,
and so in production its floor is always unknown and it refuses nothing. A
consumer resuming from a collected position then meets a 404 on a segment
that is gone — ADR-0020's failure mode, with no name on it.

The floor has to reach the consumer, and three constraints decide how:

1. **Non-negotiable 6.** The consumer runs once per SHARD. Anything a consumer
   reads for itself is a request per shard.
2. **NFR-2 and M8's criterion 3.** An idle pod issues exactly the requests its
   leases and buffers owe. A timer that refreshes the floor is a request per
   interval on every idle pod, for ever.
3. **The ingester and the plugin deploy independently.** Every type in
   `format` refuses an unknown magic or version rather than skipping it, so a
   frame an old consumer does not know would make it refuse the whole answer.

## Decision

**The floor is a frame of its own on the subscription's poll answer, sent to
any poll that asks for it, asked for only when a shard RESUMES and for a
bounded number of polls, read by the serving pod from the newest checkpoint
through a per-pod cache that is refreshed on demand -- and checked by the
plugin on the path a resumed shard reads through, where a refusal pauses the
shard visibly.**

- **A new format type, `RetainedFloor`** — magic `BPRF`, v1: the stream as two
  big-endian longs and a partition, then `oldestRetainedOffset` — with a golden
  file and a decoder that refuses rather than guesses, by the rules every type
  in `format` already follows (ADR-0053's list).
- **A frame, not a field on `SubscriptionEvent`.** An event exists only when a
  segment is pushed. The floor matters most on RESUME, and a resumed stream is
  often quiet; a floor that could only ride on a push would not arrive at the
  moment it is needed.
- **Negotiated: the poll carries `floor=1`.** A consumer that does not ask never
  receives the frame, so an old plugin against a new ingester is untouched. A
  new plugin against an old ingester receives none and its floor stays
  unknown, which is exactly today's behaviour. Neither side needs the other
  upgraded first.
- **Asked for on a resume, for a bounded number of polls.** A shard that
  resumes from a stored position asks for a FRESH floor; the consumer sets
  `floor=1` on its next polls until one arrives or `MAX_FLOOR_ASKS` polls
  have asked, and the ingester answers any poll that asks. A shard tailing the
  stream asks for nothing, so a node whose shards are all tailing costs the
  serving pod no read at all. ⚠️ **TWO EARLIER SHAPES WERE WRONG, IN
  OPPOSITE DIRECTIONS, AND REVIEW FOUND BOTH.** Sending only on the answer
  that created a session lost the floor to one dropped response. Asking until
  a floor arrived and then never again fixed that and broke two things: a
  stream the ingester had no floor for asked FOR EVER -- a store read per
  refresh interval on an idle pod, the timer this ADR rejects below -- and a
  stream that had a floor never refreshed it, so a reset hours later was
  checked against a floor GC had long since passed.
- **The refusal fires on the plugin's read path, and only a FRESH floor can
  clear it.** A resume (`readNext` with a pointer) records the position it
  asked for; both `readNext` overloads refuse it whenever the floor held is
  above it, throwing out of `readNext` -- which research 02 §6 says PAUSES
  that shard, readable in `_ingestion/_state`. A held floor may refuse, since
  floors only rise and it is a true lower bound; only a floor reported AFTER
  the resume asked may clear it. The floor arrives asynchronously and can land
  after the resume's first read, so the check repeats -- one field, no
  request -- until then. ⚠️ An earlier draft
  of this change left the refusal with no production caller at all, and its
  backlog row claimed otherwise; review found it.
- **The serving pod reads the checkpoint, not the consumer.** It costs the
  checkpoint pointer's stat and get — two requests — cached per pod and
  refreshed at most once per refresh interval, and ONLY when a poll that asks
  for the floor arrives -- which happens after a resume and for a bounded
  number of polls. So the rate is bounded by pods over the interval, and it
  is zero on a pod where no shard resumes.
  The epoch whose chain is read is the one this pod last committed under, which
  it already holds without a request; ⚠️ an earlier draft of this bullet
  budgeted a lease read for it, which the implementation does not make. So the rate is bounded by pods over the
  interval, it is zero on a pod where no session starts, and it scales with
  nodes and nothing else.

## Consequences

- **The floor a resume is cleared by can be LOW, never high.** It is a
  checkpoint's value fetched after the resume asked -- at most one refresh
  interval plus one checkpoint cadence old -- and a floor only rises. Low refuses less than it should: a consumer may still meet
  a 404 on a segment collected in that window. It is never refused for records
  still in the bucket.
- **After a takeover a pod may read the PREVIOUS term's checkpoint**, because
  the epoch it knows is the one it last committed under. That checkpoint's
  floor is still a true lower bound — collection never un-deletes — so this
  too errs low. A pod that has never committed knows no epoch, reads nothing,
  and its floor is unknown, which refuses nothing.
- **The consumer-side seam gains one method**, `SubscriptionTransport.Listener.
  onRetainedFloor(key, offset)`, a no-op by default: a listener that ignores
  the floor refuses nothing, which is correct for every caller that does not
  own a `ConsumerClient`. The plugin's node-level router routes it by key, as
  it already routes deliveries.
- **The end-to-end join — GC deletes, the floor travels, the consumer is
  refused — is M8's criterion 22 and M8.30's**, not this decision's.

## Alternatives rejected

**Piggyback on the commit acknowledgement.** Every push follows a commit round
trip, so the leader could return the floor with each ack at zero extra
requests. It changes the `Sequencer` seam and ADR-0053's forwarded-commit
response, and a floor that only moves when someone commits does not move on
the idle stream that is resuming — which is the case that matters.

**A timer on every pod that re-reads the checkpoint.** Correct and simple, and
it breaks NFR-2 on every idle pod: a request per interval for a number that
changes when GC runs.

**The consumer reads the checkpoint itself.** A request per shard per
interval, which non-negotiable 6 forbids by name.
