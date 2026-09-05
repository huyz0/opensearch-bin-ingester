---
name: research
description: Use before any web search or design argument about object storage, cost models, OpenSearch ingestion, WarpStream/AutoMQ, CAS coordination or the Java runtime.
---

# Research corpus

`docs/research/` is 21 documents compiled before any code existed: the cost
model, prior art read from source, the OpenSearch SPI, and this project's own
design decisions. ⚠️ **Do not read it wholesale, and do not re-derive what it
already answers.**

## How to navigate

1. **Start at [`docs/research/README.md`](../../../docs/research/README.md).**
   It carries the five key findings, a tier table, and a task-based routing
   table. Use the routing table.
2. **Read one document's relevant section.** One section usually suffices.
3. **Check
   [`50-open-questions.md`](../../../docs/research/50-open-questions.md)** before
   re-opening anything. As of 2026-08-30 **every question is answered** — ten as
   ADRs in [`decisions/`](../../../docs/internal/product/decisions/), the rest in
   place with evidence. What remains is six constants that need running code;
   they are *deferred to measurement*, not open design.

## What is where

| Question about | Document |
|---|---|
| Why this project exists; request pricing; the design rules R1–R11 | `00-problem/02-cost-model.md` |
| Constraints C1–C9, non-goals, success criteria | `00-problem/01-goals-and-constraints.md` |
| WarpStream, AutoMQ, KIP-1150, SlateDB, Quickwit | `10-prior-art/` |
| The OpenSearch SPI, the poll loop, plugin packaging | `20-opensearch/` |
| Segment format, key filters, CAS/ordering, tailing, AZ topology, retention, store SPI, resilience, consumer position, fetch modes | `30-design-space/` |
| Helidon/JDK/virtual threads, streaming I/O, benchmarking | `40-implementation/` |

## Reading the markings

- **Status:** `stable` (verified against source or vendor docs) · `draft`
  (correct in outline) · `proposal` (our design, not yet validated).
- **⚠️ Revision banners.** Several documents carry ⚠️ notes where a later
  finding overturned an earlier one — most importantly the scale revisions of
  2026-08-29/30, which reversed the key-filter encoding and the read-path
  recommendation. **When an early conclusion and a revision banner disagree, the
  banner wins.**
- Numbers are computed, not quoted, unless a source is cited.

## Grounding on disk

Claims are grounded in code where possible. Verify rather than trust:

- `/home/tuong/work/OpenSearch` — OpenSearch 3.8.0 source (the ingestion SPI,
  `DefaultStreamPoller`, `ingestion-fs`, `repository-s3`)
- `.tmp/automq` — AutoMQ source (object format, `LogCache`, the zerozone
  subscription). Re-clone:
  `git clone --depth 1 --filter=blob:none https://github.com/AutoMQ/automq.git .tmp/automq`

## If the corpus does not answer it

Then research it — and **write the answer back**. A finding that lives only in a
conversation is a finding that will be researched again. If a design thread runs
more than a couple of exchanges, it has earned a home in the corpus: add or
update a document, update its one-line takeaway in the README, and if the
finding touches request rates, **re-check the arithmetic in the cost model**.
Keeping the cost model true is what keeps the rest honest.
