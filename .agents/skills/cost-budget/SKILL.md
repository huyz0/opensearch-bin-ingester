---
name: cost-budget
description: Use whenever a change touches how objects are written, read, listed or discovered — and before claiming any change is cost-neutral.
---

# Cost budget

The product converts a network bill into an API bill
([`docs/research/00-problem/02-cost-model.md`](../../../docs/research/00-problem/02-cost-model.md)).
A change that quietly adds a request per record is a defect that **passes every
other gate**, and the only place it shows up otherwise is an invoice.

## The rules a change must not break

| # | Rule |
|---|---|
| R1 | Bundle all indices and partitions of a pod into one object per flush |
| R1b | ⚠️ **The flush interval is adaptive and is the primary dial** — a flat 250 ms costs $311/mo against $15.55/mo at a 5 s ceiling ([ADR-0017](../../../docs/internal/product/decisions/0017-every-pod-writes.md)) |
| R2 | Never `LIST` on a hot path — recovery and GC only |
| R3 | **Idle consumers issue zero requests** |
| R4 | Always coalesce adjacent reads |
| R5 | One fetch per object **per node**, shared by every shard |
| R6 | Commit/metadata write rate is independent of index count |
| R7 | Prefer one speculative range read over a two-step footer-then-index read |
| R8 | Retention is a cost dial; default hours |
| R9 | Every store operation is counted and attributable |
| R10 | Read cost scales with node count — serve reads from the ingester nodes |
| R11 | Cache where fan-in is high, not where it is low |
| R12 | The store is fronted by a governor that **refuses**, not merely counts |
| R13 | Govern on **ratio-to-expected**, not a fixed rate — alarm 3x, hard-stop 10x |
| R14 | ⚠️ **Never refuse a data write or a commit.** Backpressure upstream instead |
| R15 | `LIST` has its own hard ceiling (~1/s sustained), independent of the ratio |
| R16 | Count per index **in memory**; never export it as a metric label |
| R17 | `binstore_governor_refusals_total` is zero in steady state |
| R18 | **Never relax a budget to make a check pass** — moving one is an ADR |

## The invariant to check

```
request rate may scale with:      segments, AZs, nodes
request rate may NEVER scale with: records, shards, partitions, indices, documents
```

⚠️ If a partition count, shard count, index count or record count appears in a
request-rate expression, the design is wrong. That is the whole architecture in
one line.

## How to check

1. **Read the diff for new store calls.** Every `get`/`put`/`list`/`stat` added
   or moved inside a loop is a finding until proven otherwise.
2. **Run the counting harness.** `CountingBinStore` records `{op, count, bytes}`
   per purpose; the assertions live with the tests:
   - `requestsPerMiBWritten() < 0.30`
   - `listRequests() == 0` on any hot path
   - **`idleCluster.totalRequests() == 0`** — the most valuable assertion in the
     project, and the one failure invisible to every functional test
   - read requests scale with node count, not shard count
3. **State before/after in the commit body** when the change touches the request
   path. "No change" is a valid answer and should be said.

## When a budget genuinely must move

Then it is a decision, not an adjustment: write an [`adr`](../adr/SKILL.md) with
the number, the alternative rejected, and what the new budget buys.
⚠️ **Never relax the threshold to make the check pass.**
