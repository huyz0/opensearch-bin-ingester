# 0023. NFR-2's idle-cost zero is verified by construction, not by a runtime assertion

Status: accepted
Date: 2026-09-01
Requirements: NFR-2
Research: docs/research/30-design-space/04-discovery-and-tailing.md §2a, §2b

## Context

NFR-2 requires **zero** object-store requests from an idle consumer. M1.16
tried three times, at two tiers, to prove this with a runtime test — a
1,600-consumer T1 test, a `tick()`-driven deterministic variant, and a
20-shard T4 test — and all three were withdrawn after review, along with two
more test-level assertions of the same shape found already sitting in the tree
(`SearchableIT`, `SubscriptionHubTest`) and a fourth removed outright
(`EndToEndTest`).

Every attempt failed for the same reason. ADR-0004 already decided that reads
are served by same-AZ ingester nodes, streamed through, never buffer-then-
forward — the consumer never holds a `BinStore` reference at all. Verified
directly: neither `plugin/src/main` nor `client/src/main` imports
`binjava.binstore`, anywhere. So a request-count assertion in any consumer-side
test can only ever observe a store the test itself constructed and wired in
for the purpose of counting — never one the consumer path can reach. The
assertion cannot fail regardless of whether the consumer is correct, buggy, or
not running at all. An assertion that cannot fail is not evidence, and no
amount of additional test engineering changes that: the only way to make the
zero *runtime-falsifiable* is to first give the consumer a store reference to
call — which is the regression NFR-2 exists to prevent, not a step toward
proving its absence.

This is not a gap in test-writing skill. It is what a genuinely-zero surface
looks like from the outside: nothing to instrument, because there is nothing
there.

## Decision

NFR-2's idle-cost zero is verified **by construction**, via a dependency
check, not by a runtime assertion:

> No class under `plugin/src/main` or `client/src/main` imports
> `binjava.binstore`.

This is checkable today by hand (`grep -rn binjava.binstore plugin/src/main
client/src/main` — no matches) and is exactly the shape `check-module.sh`
already enforces for architecture rule 4 (nothing depends on `http`). The claim is stronger than the grep: `plugin/build.gradle.kts` and `client/build.gradle.kts` do not put `binstore-spi` (the module that defines the `binjava.binstore` package) on either module's **main** classpath at all — `client` depends only on `format`, and `plugin` depends only on `client` plus `binstore-backends`/`ingest` as `testImplementation`. So this is not "no one happens to have imported it yet"; the package is not resolvable from that classpath to import. Rung 3 of
`gate-design`'s ladder — "derive it from a source of truth" — beats rung 7
("ask an agent") here: whether a class *imports a package* is a predicate over
files in the tree, not a judgement call, so it belongs in a script the way
`check-module.sh`'s existing dependency checks do. Adding that check is
`M1.16e`; until it lands this ADR is the record of the check, run by hand.

The **liveness** half of the withdrawn tests is real and stays: 1,600
registered subscriptions all receiving a control push, and (at T4)
`ShardFanOutIT`'s twenty shards all polling, decoding and indexing. Those
assertions can fail — a dead poller, a routing bug — and they do the actual
regression-catching work this milestone needs. Only the request-*count* zero
was unfalsifiable, and only that half is retired to construction.

## Alternatives considered

- **Keep trying test constructions until one works.** Rejected after four
  attempts across two tiers found the identical wall each time. There is no
  fifth construction to try; the property is unconditionally true given
  ADR-0004, which is exactly why no test can conditionally fail it.
- **Give the consumer a store reference so the zero becomes observable.**
  Rejected: this is not a test change, it is a production regression — the
  thing that currently cannot happen (a consumer-side object-store request)
  would become possible, purely so a test could watch for it not happening.
  The cure is worse than the disease it treats.
- **Leave the unfalsifiable assertions in place, uncommented.** Rejected,
  and this is what M1.16b spent four review rounds undoing: a green test
  asserting NFR-2's exact words reads as evidence to the next person who greps
  for it, right up until they trace what it can actually observe. Silent
  false evidence is worse than an honest gap, which is why the SPEC and
  backlog now say "not provable as written" in every place that used to
  claim otherwise, rather than staying quiet about it.
- **Descope NFR-2 from M1 entirely, to M5** (where fetch-mode consumers gain a
  store reference for catch-up replay, per ADR-0004). Considered and rejected
  for M1's completion condition specifically: the property already holds at M1
  — checkable now, by the import grep above — so descoping would understate
  what is true today. M5 is instead where the request-count *test* first
  becomes meaningful, once a consumer has a store to not-fetch from; that is
  recorded as the target for `M1.16e`'s eventual runtime companion, not a
  reason to withhold the construction-level claim now.

## Consequences

**Makes easy:** M1's completion condition can state NFR-2 as met, honestly,
without a runtime gate that would have had to be either unfalsifiable or a
regression to write.

**Makes hard:** nothing regresses this silently in the interim, because
nothing enforces the import-absence check yet — `M1.16e` closes that gap.
Until it lands, a future PR could add a `BinStore` import to the consumer path
without any gate objecting, and the only trace of the rule is this ADR and the
SPEC's prose.

**Forecloses:** treating "an idle test passes" as evidence of anything on the
consumer path, project-wide. The pattern this ADR names — a request-count
assertion that cannot fail because the code under test cannot reach the thing
being counted — is worth recognising the next time it appears, in any module,
not just this one.
