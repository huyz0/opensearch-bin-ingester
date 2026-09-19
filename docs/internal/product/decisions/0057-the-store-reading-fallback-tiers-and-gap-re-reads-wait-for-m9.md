# 0057. The store-reading fallback tiers and gap re-reads wait for M9

Status: accepted
Date: 2026-09-20
Requirements: FR-10, NFR-4, NFR-13
Research: docs/research/30-design-space/04-discovery-and-tailing.md

## Context

Doc 04 § 3 gives the consumer a fallback ladder: push, reconnect, poll the
commit chain, recover from the newest checkpoint, and -- only by an explicit
recovery action -- LIST the data prefix. M5.18 shipped it as a policy nothing
executed (`FallbackLadder`), and M8's criterion 20 asked for it to execute on
a real delivery gap, with the GETs its tiers cost COUNTED.

Two facts stand between M8 and that criterion as written:

1. **Tiers 2 and 3 read the object store, and run exactly when no ingester is
   reachable.** Architecture rule 2 keeps any cloud SDK out of the plugin, and
   nothing lets it sign a request. A grant needs an ingester to issue it, and
   there is none at that moment.
2. **Re-reading a missed window needs a catch-up read path, and there is
   none.** A subscription is a live tail: the ingester has no replay from an
   offset. That path is M8.24's -- resume from `batch_start` without starving
   the tail -- which the user deferred to M9 on 2026-09-20.

## Decision

**M8 executes the ladder's automatic tiers 0 and 1 on a real connection loss;
tiers 2 and 3, and the re-read of a gap's missing window, are M9's.**

- `HttpSubscriptionTransport` asks `FallbackLadder.tierFor` for its tier at
  every transition it observes -- the first answer after a loss is PUSH, a
  lost poll is RECONNECT -- and counts the tiers it enters. That is the
  ladder executing, over a real socket (`LadderExecutionTest`).
- Tiers 0 and 1 cost zero GETs by construction: the client holds no store.
  `TENS_OF_GETS` stays MODELLED, and says so, because nothing executes tier 3.
- A gap stays what M6.1 made it: detected, attributed (local drop or
  upstream), and visible -- never silently indexed past.

The user chose this on 2026-09-20 over a SigV4 signer in the plugin (a
credential in the OpenSearch JVM) and long-lived pre-issued chain grants
(amending ADR-0041's short TTL).

## Consequences

- An ingester outage longer than the reconnect backoff leaves consumers in
  RECONNECT, indexing nothing new, until an ingester returns: latency, not
  loss, and the records are durable in the store. Doc 04's claim that an
  outage "degrades latency, never correctness or availability" holds for
  correctness and not yet for availability.
- M9 owns: the catch-up read path (M8.24), the plugin-side store access tiers
  2 and 3 need, and with them re-reading a gap and measuring `TENS_OF_GETS`.
