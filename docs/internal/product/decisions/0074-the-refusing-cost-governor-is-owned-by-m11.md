# 0074. The refusing cost governor is owned by M11, ahead of priority lanes

Status: accepted
Date: 2026-09-26
Requirements: FR-21, FR-18, NFR-1, NFR-3
Research: docs/research/30-design-space/15-cost-governor.md

## Context

FR-21 asks for a cost governor. Research 15 and cost.md rules 12-17 define
what "governor" means here, and it is more than a counter:

- it **refuses**, and it refuses on a **ratio to expected**, not a fixed rate:
  alarm at 3x, hard-stop discretionary work at 10x (cost.md 12-13);
- it **never refuses a data write or a commit**; those get backpressure
  upstream -- a `429`, or a blocking in-process call (cost.md 14, research 15
  §3);
- **LIST has its own hard ceiling**, about 1/s sustained, whatever the ratio
  (cost.md 15);
- reads queue and then degrade; GC, compaction and the recovery sweep defer at
  once (research 15 §3);
- attribution by `(op, purpose, domain, index)` is kept **in memory** and
  reaches an operator through top-K log events and `/admin/cost`, never as a
  metric label (cost.md 16);
- `binstore_governor_refusals_total` is zero in steady state (cost.md 17);
- beyond the hard stop, a kill switch -- triggered at 100x expected by
  default, or by any sustained LIST above the ceiling -- halts all
  discretionary work (GC, compaction, recovery sweeps, prefetch), keeps the
  write path alive under backpressure, and pages the top three
  `(purpose, index)` pairs (research 15 §5).

Why counting is not controlling, in research 15 §1's numbers: a regression
reinstating per-shard polling costs **$1,728/hour as GETs and $21,600/hour as
LISTs**, and a counter that only records it turns a one-line bug into a
five-figure bill before anyone reads a dashboard.

What exists today is the counting half. `CountingBinStore` counts by operation,
`PutPurposeCounts` (M9.55) splits PUTs by purpose, `CostMeter` turns counts
into requests per MiB and dollars (M9.1), and the macro harness and nightly
workflow assert budgets on the rig (M9.8, M9.16, M9.56). **Nothing refuses a
request.** M9's SPEC and `VERIFIED.md` record that the refusing half -- cost.md
rules 12-17 -- is owned by no milestone once M9 ends, and M9's re-plan asked
M10 to give it one. The user scoped M10 on 2026-09-26 to the proxy route and
the full NFR-5 proof, moving priority lanes (FR-18) to M11 and fast mode
(FR-17) to M12. The question is where the governor goes in that order.

## Decision

**M11 owns FR-21's refusing governor, and builds it first, before priority
lanes.** M11 becomes "Admission: the cost governor, then priority lanes". Its
completion condition gains cost.md rules 12-17 in full: *a store decorator
that computes each purpose class's ratio to expected and alarms at 3x and
defers discretionary work (GC, compaction, recovery sweep) at 10x; that never
refuses a segment PUT or a commit, applying backpressure instead; that holds
LIST to a hard ceiling of about 1/s; that queues and degrades reads; that keeps
`(op, purpose, domain, index)` attribution in memory, surfaced by top-K log
events and `/admin/cost` and never as a metric label; that exports
`binstore_governor_refusals_total`; and a kill switch, triggered at 100x
expected or by sustained LIST above the ceiling, that halts all discretionary
work (GC, compaction, recovery sweeps, prefetch), pages the top three
`(purpose, index)` pairs, and leaves the write path (segment and commit-delta
PUTs, lease renewals) running under backpressure. Proved by research 15 §6's
three tests -- a fault-injecting store reporting a fabricated rate, asserting
EACH class degrades as §3 says (writes and commits backpressured, never
dropped; reads queued then degraded; GC, compaction and sweeps deferred; LIST
held to its ceiling); a regression reinstating per-shard polling (the blocking
`readNext` removed) refused within seconds while writes still succeed; and a
normal-load run ending with `binstore_governor_refusals_total == 0` -- plus a
test that the kill switch's trigger halts discretionary work and pages.*

⚠️ **The governor and the lanes share an ORDER, not a mechanism.** FR-18's
lanes schedule *records* by priority through admission, flush, commit and
push; the governor's classes are *store requests*, and every lane, bulk included,
is a data write the governor may never refuse. The one real touch point is
backpressure: a governor `429` arrives at the same front door as ADR-0014's
per-lane admission, and M11's spec must say which answers first. The governor goes first because it is protection the existing path
already needs, and M11 is the earliest milestone that can build it without
renumbering the user's plan.

## Alternatives considered

- **M12, with fast mode.** Rejected: fast mode adds a second write path (a
  WAL, FR-17) whose request rate is unmeasured, and building it before the
  governor ships a new source of requests into a system that can only count
  them. Every milestone that ships first inherits the $21,600/hour exposure
  above for as long as it runs.
- **Its own milestone between M10 and M11.** Considered and not chosen: it
  would work equally well technically. It was not chosen because it renumbers
  the milestones the user just ordered, for no gain in when the governor
  lands -- M11-first builds it at the same point.
- **Inside M10.** Rejected by the user's scoping of M10, and by size: M10
  already runs 25 task rows, and rules 12-17 plus a kill switch are six
  mechanisms with their own review surface -- a milestone's worth, not a
  harvest row.
- **Leave it unowned.** Rejected: an obligation with no owner is how M9 found
  it. ADR-0052 is the precedent for the remedy: work whose evidence another
  milestone depends on is given an owner and an order, not left floating.

## Consequences

- The roadmap's M11 row names the governor first and priority lanes second,
  with the completion condition above, and keeps the catch-up-on-any-pod item
  M10.22's disposition added to it. Nothing in M10 changes; M10's criterion 10
  is met by this record.
- M1.3b, an M1 backlog row that named the same `GoverningBinStore` and was
  never built, is superseded by this record rather than left as a second
  owner.
- Until M11 lands, production is protected from a runaway only by the counting
  half and by review. That is the state M9 shipped, stated rather than
  assumed.
- M11's spec must decide the expected-rate source for each class (research 15
  §2) and the purpose classes themselves. ⚠️ `PutPurposeCounts` is not that
  vocabulary: it splits only PUTs, by key (data, commit, checkpoint, lease,
  other). The classes the governor defers or ceilings -- GC, compaction,
  recovery sweep, prefetch, reads, LISTs -- are new, and attribution by index
  is new.
