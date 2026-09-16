<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0046. The pending pool covers an unknown index, not an unpushed rollover

Status: accepted
Date: 2026-09-17
Requirements: FR-13, FR-19
Amends: [ADR-0015](0015-routing-registration-and-aliases.md) §3
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §4

## Context

ADR-0015 §3 introduces a bounded pending pool for records whose placement the
ingester cannot yet compute, and names two triggers:

> a record arrives for an index whose shard count the ingester does not yet know
> (**never registered, or a rollover the plugin has not yet pushed**)

and claims the consequence:

> This makes the alias-rollover staleness window (§4) shrink from "records land
> in the previous index" to "records wait a few milliseconds and land in the
> right one".

⚠️ **The second trigger is not implementable as stated, and M6's spec review is
where that surfaced.** The two triggers are not the same kind of thing:

- **Never registered** is a state the ingester can *observe*: it looks the index
  up, finds nothing, and knows it does not know.
- **A rollover the plugin has not yet pushed** is a state the ingester cannot
  observe at all. The alias still resolves to the previous concrete index, whose
  shape *is* registered and *is* correct **for that index**. Nothing in the
  ingester distinguishes "this mapping is current" from "this mapping was
  current a millisecond ago" — that is precisely the information the push is
  carrying, and it has not arrived.

Making the second trigger real would mean the ingester asking OpenSearch whether
an alias still points where it thinks, per write or on a timer. ADR-0015 §2
exists to avoid exactly that: no new endpoint, no new credential, no producer or
ingester talking to the cluster out of band. A poll would also put a request
rate on an idle cluster, which is NFR-2's shape one layer up.

## Decision

**The pending pool covers the unknown-index case only.**

1. A record for an index the ingester has **no registration for** waits in the
   bounded FIFO pool and is placed when registration arrives, or is **rejected**
   after `pendingTimeout` — ADR-0015 §3 unchanged for this case, which is the
   one the producer-starts-before-the-plugin-connects race needs.
2. A record for an index the ingester **does** have a registration for is placed
   immediately, against that registration. If a rollover has happened and the
   push has not yet arrived, the record lands in the **previous concrete
   index** — which is ADR-0015 §4's documented bounded staleness, and which §4
   already calls normal for time-series rollover.
3. **ADR-0015 §3's shrink claim is withdrawn.** The staleness window is bounded
   by the plugin's push latency — a cluster-state listener firing on change,
   typically milliseconds — not by the pending pool.

## Consequences

- **The window is milliseconds and it is real.** Records written to an alias in
  the gap between a rollover and the plugin's push are committed to the previous
  index's streams and keep those offsets. Nothing is repartitioned, because
  ADR-0015 §4 makes stream identity `(indexUUID, partition)` — per concrete
  index — so those records are consistent, readable and searchable in the index
  they landed in. What they are not is *in the new index*.
- **For time-series rollover this is what an operator already expects**: the
  previous index is still queried through the read alias for its whole retention
  window.
- **It is documented rather than discovered**, which is the obligation §4 states
  and this ADR discharges by naming the mechanism that bounds it.
- **A deployment that cannot tolerate the window rolls over on a boundary the
  producer also knows** — writing to the concrete index name rather than the
  alias makes the placement exact, at the cost of the producer knowing the
  rollover.

## Alternatives considered

- **Implement §3 as written by having the ingester detect staleness.** Rejected:
  it needs the ingester to ask OpenSearch, which ADR-0015 §2 rejects for the
  producer for the same reasons — a credential, an endpoint, and a coupling to
  cluster topology. ⚠️ **And the number settles it**: "per write" means per
  RECORD, not per request — a `_bulk` of 10,000 records for an aliased index
  would be 10,000 cluster questions, against a design whose whole point is that
  a write costs a fraction of a request. A poll instead trades that for a
  request rate on an idle cluster, which is NFR-2's shape one layer up.
- **Have the plugin push BEFORE the rollover completes.** Rejected: the plugin
  learns of a rollover from the cluster state, which is published *after* the
  change; there is no pre-commit hook, and inventing one means patching
  OpenSearch.
- **Hold every write for an alias until a fresh push confirms the mapping.**
  Rejected: it converts a millisecond window at rollover into a round trip on
  **every** aliased write, and the ack path is what this project spends its
  latency budget on.
- **Re-place records that are buffered but NOT YET COMMITTED when the push
  arrives.** ⚠️ The strongest alternative, and it needs no question to
  OpenSearch: ADR-0001 assigns ordering at commit, so a record still in the
  accumulator can be moved between partitions exactly as a pending one can.
  Rejected on what it actually buys: the accumulator's flush interval is
  250 ms at the floor, the push follows the cluster-state change by
  milliseconds, and the two windows overlap only when a rollover lands inside a
  flush — so it removes part of an already-millisecond window, at the cost of a
  second placement path that runs on the flush hot path and a record that can
  change partition after the producer's request returned 202. It becomes worth
  revisiting if a deployment lengthens the flush interval toward the 5 s
  ceiling, and this paragraph is where the next reader starts.
- **Let the pool cover both and accept that the rollover case never fires.**
  Rejected as the worst option: the code would carry a branch nothing reaches,
  the ADR would keep a claim no test could pin, and the next reader would
  reasonably believe the window was closed. M6's spec review caught exactly that
  belief in three places in one draft.
