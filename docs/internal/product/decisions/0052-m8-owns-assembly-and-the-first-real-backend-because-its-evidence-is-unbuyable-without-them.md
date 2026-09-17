# 0052. M8 owns assembly and the first real backend, because its own evidence is unbuyable without them

Status: accepted
Date: 2026-09-18
Requirements: NFR-8, NFR-9, NFR-11, NFR-3, NFR-2, FR-11, FR-12
Research: docs/research/30-design-space/08-failure-domains-and-resilience.md

## Context

M8's completion condition is *"chaos matrix passes, including `SIGSTOP` gray
failure and AZ partition; RPO 0 demonstrated"*. Two things it needs have no
owning milestone, and both were recorded as open before this ADR rather than
discovered by it.

**There is no production `main()` in the tree**, and it is not alone: M5 shipped
mechanisms nothing constructs, M5.6e records that the production
`SequencerTransport` has never existed, M6.19 records the routed path wired into
no deployable server, and M7's milestone review found more — including the
commit chain itself, which **no `src/main` method can even produce** (M7.25:
`CommitLog.recover()` returns `void` and `ChainReplay.Result` discards the
deltas it reads).

⚠️ **THE SET IS NEITHER LISTED NOR COUNTED HERE.** It is M8's SPEC
§ *The unwired set*, which is the only copy, with a closing predicate per entry.
Three earlier drafts of this paragraph enumerated it and counted it — "eleven",
then "six more" — each short, and each disagreeing with the table. That is an
argument for one copy, not an argument about arithmetic.

**The first real object-store backend has no milestone either.** The roadmap's
deferred table says so in as many words, and the M9 row already argues the
consequence: booking it after M8 means M8's chaos matrix, AZ partition and RPO-0
evidence are demonstrated against `MemoryBinStore` and `LocalFsBinStore` —
"modelled rather than measured, which is the thing M9's own condition exists to
prevent". Both backends answer `presignedUrls=false`, neither has a network, and
neither can be partitioned away from a pod.

The numbers that make this decisive rather than tidy-minded:

- A chaos matrix over a system nothing assembles kills **processes that run
  none of the mechanisms under test**. `SIGSTOP` on a pod that runs no retention
  loop cannot show GC stopping; an AZ partition cannot show that a fenced GC
  deletes nothing, because nothing composes `LeasedGc` with `RetentionPass`.
- That composition is not hypothetical-safe: M7.10's commit body records,
  MEASURED, that composing them the obvious way **"compiles, passes, and deletes
  unfenced"**. A silent data-loss path, in the one seam M8 exists to break.
- **RPO 0 (NFR-8) is a statement about acked writes surviving a process death.**
  A test that never starts a process, and whose store is a `HashMap` in that
  process's heap, cannot distinguish RPO 0 from RPO ∞.
- M7's GC cost budgets — 0 LIST, 0 GET, ≤1 DELETE per 1,000 keys — are asserted
  around `SegmentGc` and are **unproven of a pass**, because the only code that
  reads deltas is the recovery walk, at one LIST per 1,000 deltas plus one GET
  per delta. Assembly is where that budget is either met or violated, and it is
  currently neither.

## Decision

**M8 owns assembly and the first real backend, as its first tasks, before any
chaos task.** Its completion condition is unchanged; what changes is that the
milestone is decomposed so that the thing being chaos-tested exists first.

Three consequences of that, stated so they can be planned against:

1. **A production `main()`** that constructs the ingester — writer, reader,
   sequencer, subscription hub, retention loop and GC lease — from configuration,
   and a way to run it as a process. Every row of M8's SPEC
   § *The unwired set* — the single list, which this ADR deliberately does not
   restate — is wired here or is explicitly deferred again **with its reason in
   `VERIFIED.md`**.
2. **One real object-store backend**, exercised against a real endpoint
   (MinIO in the test harness, S3-compatible), so that a partition, a slow
   endpoint and a signed URL are things that can happen rather than things that
   are modelled. This closes the roadmap's unassigned row and ADR-0041's
   deferred presign obligations (M5.37, M5.42).
3. **The chain's in-memory source is named before any GC loop is wired**
   (M7.25). Assembly is not permitted to reach for `recover()`'s walk per pass;
   if the sequencer's in-memory chain is not exposed, exposing it is a task.

⚠️ **This is an ordering decision, not new scope.** Every requirement M8 already
owned it still owns. What is refused is demonstrating them against a system that
does not exist.

## Alternatives considered

- **A separate assembly milestone between M7 and M8.** This is what M7's
  milestone review proposed and it is the closest call. Rejected because it
  splits one completion condition across two milestones: an assembly milestone's
  own condition would have to be *"the thing starts"*, which is not checkable by
  anything other than an opinion until something breaks it — and the thing that
  breaks it is M8's chaos matrix. The roadmap's own M9 row makes this argument
  about the backend; it applies identically to the process. Numerically: of the
  unwired mechanisms enumerated above, **most are only observable under a
  fault**
  (fencing, lease loss, reconnect, refusal, alarm), so an assembly milestone
  would ship them with the same "met at the class" evidence M7 had to write.
- **Keep M8 as specified and accept `MemoryBinStore` evidence.** Rejected, with
  its number: `MemoryBinStore` is a `ConcurrentHashMap` in the killed process's
  own heap, so its RPO is definitionally 0 for survivors and definitionally ∞ for
  the killed pod, independent of any code this project wrote. The measurement
  would be of the fixture.
- **Fold assembly into M9 with the benchmarks.** Rejected: M9 measures cost and
  latency, and measuring either on a system whose failure behaviour is unknown
  produces numbers nobody can act on — and it leaves M8 with nothing to break.
- **Build the backend in M8 but keep assembly out, testing the store through
  the conformance suite only.** Rejected: the conformance suite already passes
  against two backends, so it buys the SPI's shape and not the deployment's
  behaviour. The failure modes M8 names — gray failure, partition — are
  properties of a *process talking to an endpoint*, and neither half alone has
  one.

## Consequences

- **M8 is the largest milestone in the roadmap**, and says so. That is the cost
  of eight milestones of seams: the harness bought testability at every step and
  deferred the assembly each time, each deferral defensible. ⚠️ The set is enumerated in M8's SPEC § *The unwired set* and is not counted here, for the reason that section gives.
  ⚠️ **The deferrals were individually right and collectively a debt**, and this
  ADR is where it is paid rather than rolled again.
- **M8's `VERIFIED.md` inherits an obligation**: for every row of M8's SPEC
  § *The unwired set*, an evidence line saying the mechanism is now wired, or
  saying it is still not and why. A milestone that assembles the
  system and leaves a mechanism unwired without saying so is the failure this
  ADR exists to end.
- **The first real backend arrives with a container in the test path.** That is
  a build-time cost (build.md's memory caps and the Docker-off-by-default rule)
  and it is what makes `PresignConformance`'s capable branch and M5.42's
  signing-failure case executable at last.
- **It forecloses discovering assembly bugs during the cost benchmarks.** M9's
  numbers are then measured on the same assembled system M8 broke, which is the
  only arrangement in which they describe the product rather than a harness.
- ⚠️ **What it does NOT buy is multi-AZ or multi-node deployment reality.** The
  chaos harness partitions processes on one machine. NFR-9's `EndpointSlice`
  watch is a Kubernetes mechanism and M8 owns it, but the watch's own behaviour
  under a real control plane stays unmeasured until something runs on a cluster,
  and M8's evidence document must say that rather than implying otherwise.
