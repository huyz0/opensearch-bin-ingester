# 0078. Per-index quotas are pod-local token buckets on admitted bytes and records

Status: accepted
Date: 2026-09-27
Requirements: FR-21, NFR-6
Research: docs/research/30-design-space/11-multi-tenancy-and-security.md §2, docs/research/30-design-space/14-producer-contract.md §3

## Context

[ADR-0010](0010-multi-tenancy-and-security-model.md) closed Q9 and Q10 with
four isolation mechanisms, the first of which is a **per-index admission rate
limit**: a token bucket on bytes/s and records/s, answered `429` with
`Retry-After`. Research 11 §2 adds that it is "checked *before* buffering, so a
rejected write costs no memory" and "must be a map lookup, not a network
call". Its *enforcement* was deferred to land
with the governor; ADR-0075 re-homed it to M11 and said quotas "need the
apportionment to enforce anything".

Two facts constrain the mechanism:

- A `_bulk` body is STREAMED (M1.7b). Its record count is unknown until it is
  parsed, and its byte count is known up front only when the producer sent a
  `Content-Length`.
- Request cost is apportioned by run bytes
  ([ADR-0077](0077-an-index-cost-is-apportioned-by-run-bytes-from-the-segment-directory.md)),
  and is known only after the flush that bills it — seconds after the bytes
  were admitted.

## Decision

1. **A quota is a pair of token buckets per index — bytes/s and records/s —
   each with a burst of one second's rate, held by the pod and enforced at
   admission.** It is a `ConcurrentHashMap` lookup; nothing crosses a network.
2. **Checked before the body is opened, charged as it is appended.** A request
   is admitted only when both of its index's buckets are non-negative; the
   records and bytes are then charged chunk by chunk as they are appended, and
   may take a bucket into DEBT. A request is never cut off part-way, because
   the prefix already appended would be durable-bound and the producer's retry
   would have to re-send it.
2a. **A quota'd index holds at most `maxInFlight` admitted requests at once**
   (`ingest.quota.max-in-flight-per-index`, default 8), because decision 2 alone
   bounds nothing under concurrency: every request arriving while the bucket is
   still non-negative passes the check before any of them is charged, so 256
   concurrent bodies — the pod's whole in-flight budget — could all be
   admitted into one index's debt. With the cap, an index's debt is at most
   `maxInFlight` request bodies past a non-negative balance. ⚠️ **The slot is
   held until the request ENDS**, durable wait included -- not given back when
   a chunk is buffered, which is when the pod's lane permit goes back since
   M11.7 (ADR-0079). Amended by M11.8's review (P1): a slot returned at
   buffered counts only the requests parsing at one instant, and an index
   could be admitted thousands of bodies into debt before the first refusal.
   An index with no quota configured has no cap, as before M11. The index is
   the CONCRETE one: a write through an alias spends its index's bucket and
   slot (M11.8 review P2).
   ⚠️ **Amended by M13.5 for M12.4 (M12 review harvest R4): only an index
   known at admission gets a bucket, and only its requests are held to the
   cap.**
   - **Unknown names get no bucket.** A bucket per name a producer sends, never
     removed, grew with the names an unauthenticated producer cared to invent
     (security.md rule 5). So a name the catalog does not know at admission
     gets no bucket there.
   - **Its ticket tallies instead.** It is typically a routed write that will
     wait for its index's registration (ADR-0015). Its ticket tallies what it
     is charged, and binds once the name is known: at a later chunk's charge,
     or at the request's release if its only chunk came before the wait.
   - **Binding skips the checks.** Binding takes a slot in the index's bucket
     WITHOUT the cap's check or the debt check, since the request cannot be
     refused mid-body, and charges the tally. A name never registered charges nothing and is
     refused downstream.
   - **So the cap does not bound requests admitted during an index's
     registration window.** They bind past it; see § Consequences for how far.
   - **A window write binds its CONCRETE index's bucket, at that index's
     limit** (amended by M13.46). A name unknown at admission -- the index
     or an alias written before it registered -- is never handed a free
     ticket: its deferred ticket resolves the name when it binds, and takes
     the concrete index's bucket under the concrete index's own limit (its
     override, else an alias's in sorted order, else the default), or turns
     free if that limit is unlimited. Before M13.46 the name as sent was the
     key and the limit was read at admission: a window write through an
     alias bound a bucket of its own, whose debt refused nothing sent to the
     index; under an unlimited default it went uncharged, whatever override
     its index carried; and under a non-zero default it carried the default
     rather than the override.
   - **Idle buckets expire.** A bucket that is full, idle for the idle expiry
     (default 5 min) and has nothing in flight is dropped, swept at most once
     per expiry. A bucket in debt or holding a slot is never dropped, because
     dropping it would forgive the debt.
3. **Refused with `429` and `Retry-After` = the seconds until the deeper
   bucket's debt is repaid, rounded up, at least 1** -- and **`Retry-After: 1`
   for the in-flight cap's refusal** (decision 2a), because a slot frees as soon
   as one of the index's admitted requests ends (named here by M12.20, M11.0
   R4). The same one place that
   answers every other `429` sets the header (M11's H2).
4. **Configured per pod**: a default applied to every index
   (`ingest.quota.default.bytes-per-second`, `…records-per-second`), overridden
   per index by name (`ingest.quota.index.<name>.bytes-per-second`, …; the name
   keeps its dots, and a rate the override leaves unset is the default's).
   ⚠️ **The name may be an alias** (amended by M12.13, M11.8 P5): an override
   applies to the concrete index whose registration lists that alias, an
   override on the concrete name winning over one on its alias, and the first
   alias in sorted order over the rest. Before, an alias-named override matched
   nothing, silently. ⚠️ **A bucket's limit is fixed when the bucket is made**
   (amended by M13.5 for M12.4: no bucket is made for an unknown name any
   more).
   - A bucket first made by a request admitted before its index was known
     is made when that request's ticket binds, from the concrete index and
     its own limit, alias-keyed overrides included (amended by M13.46;
     decision 2a). Before, it took the limit read at admission from the name
     as sent, so an override keyed only by an alias went unapplied to window
     writes.
   - It keeps that limit until it expires idle, and a bucket expires only
     when full and idle, which a busy index may never be.
   **0 means
   unlimited, and is the default**, so a pod configured with nothing behaves
   exactly as before M11.
5. **The rate is per POD.** A fleet of N pods admits up to N times the
   configured rate for one index, as every other admission bound in this tree
   is per pod (ADR-0074's in-flight budget). The operator divides.
6. **A quota bounds bytes and records, not apportioned requests.** Under
   ADR-0077 an index's share of every segment request is its bytes' share, so
   bounding its bytes bounds its share of the bill at a steady state, without
   waiting for a flush to learn what the bytes cost.

## Alternatives considered

- **A quota on apportioned requests or dollars, as ADR-0075's "quotas need the
  apportionment" suggested.** Rejected: the cost of an admitted byte is known
  only after the flush that bills it, so the quota would refuse the NEXT
  interval's traffic for the last one's, and an index's apportioned share rises
  when OTHER indices go quiet — a producer refused because its neighbour
  stopped writing. Bytes are what the index controls. This amends ADR-0075's
  Deferred paragraph, which now points here.
- **A fleet-wide quota coordinated through the store.** Rejected: a request per
  admission, or per interval per index, is a request rate that scales with
  indices (non-negotiable 6), and research 11 §2 requires a map lookup (the
  requirement is research 11's, not ADR-0010's -- corrected by M12.20, M11.0 R3).
- **Refuse a request part-way once its bucket empties.** Rejected (decision 2):
  the appended prefix is durable-bound and the retry duplicates it.
- **Require `Content-Length` and charge it up front.** Rejected: producers that
  stream a chunked body are legal (M1.7b), and the charge would still need
  correcting for records, which no header carries.
- **Reserve a full second's burst at admission and refund the unused part on
  completion** (review round 1's other option). Rejected: under contention it
  admits ONE request per index at a time, so a quota'd index sending
  concurrent bulks well under its rate would be refused — a quota that refuses
  traffic inside itself is mis-tuned by construction (cost.md rule 17's
  argument, applied here).
- **Carry the quota in the index registration pushed by the plugin.** Rejected
  for M11: that is a wire-format change (ADR-0015's registration) for a setting
  the ingester's operator, not the OpenSearch index owner, is paying for. It
  stays a later option; pod configuration does not foreclose it.

## Consequences

- One index can no longer take a pod's whole admission budget and buffer:
  ADR-0010's first isolation mechanism is enforced.
- A pod with no quota configured is unchanged, so the feature is inert until
  used — like lanes (ADR-0074).
- A producer that ignores `Retry-After` is refused again, cheaply: the check is
  before the body is read.
- A debt is bounded by `maxInFlight` request bodies (decision 2a), each
  bounded by `MAX_BODY_BYTES` and `MAX_RECORDS`: at the defaults, 8 × 256 MiB
  of bytes admitted past the quota before every further request is refused
  until it is repaid. A tighter bound needs a smaller cap or a producer that
  sends smaller bulks; the ADR states the real number rather than a better one.
- ⚠️ **Plus the registration window (amended by M13.5 for M12.4).** Requests
  admitted before their index is known bind past the cap (decision 2a).
  - **How many.** Each holds a lane permit through its registration wait, so
    at most the pod's in-flight budget (ADR-0074, 256 at the default) are
    alive when the registration lands. With a quota configured and that
    budget at its default, that is up to 256 more bodies of up to 256 MiB
    each, charged past the cap (before M13.46, through an alias under an
    unlimited default, not charged at all; decision 2a).
  - **What the debt refuses.** Further requests to the SAME bucket are
    refused until the debt is repaid. A window write's debt is its concrete
    index's, through an alias or not (decision 2a, amended by M13.46).
  - **The first chunk.** Each such request's first chunk, up to
    `APPEND_CHUNK_RECORDS` (1,000) records, is charged to its ticket before
    the registration wait and reaches the bucket when the ticket binds.
  - **The waiting writes.** They are bounded per NAME AS SENT, so an index
    and each of its aliases get their own bound:
    - an explicit-partition write waits in place, at most
      `MAX_EXPLICIT_WAITERS_PER_INDEX` (8) per name, the rest refused `429`;
    - a routed write waits in ADR-0015's pending pool, at most one default
      segment (8 MiB) per name.
  - **The rest of the body** streams after binding, charged as it is
    appended.
  - **Bounding it by the cap instead is rejected.** A request cannot be
    refused mid-body. Refusing it at admission needs a bucket for a name not
    yet known, which is the unbounded growth M12.4 removed, or would refuse
    every write racing its index's registration, which ADR-0015 exists to
    absorb.
- Because the slot is held across the durable wait, a quota'd index has at most
  `maxInFlight` requests ADMITTED while known in flight per pod, however high
  its rate: at the defaults, 8 bodies per flush-and-commit latency. ⚠️ Plus
  the registration window's, which bind past the cap (decision 2a, amended
  by M13.5): up to the pod's in-flight budget. An index whose configured
  rate needs more concurrency than that needs a larger cap; the cap is a
  per-index concurrency limit as well as a debt bound, and only for an index
  that has a quota.
