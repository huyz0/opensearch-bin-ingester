# Index registration API

**Serves:** FR-16 · **Decided by:** [ADR-0015](decisions/0015-routing-registration-and-aliases.md)
(mechanism), [ADR-0006](decisions/0006-partition-assignment-and-the-routing-invariant.md)
(partitioning modes), [ADR-0009](decisions/0009-replication-mode-segment-replication-primary-only-ingest.md)
(replication mode).

ADR-0015 settled *how* registration reaches the ingester: the plugin pushes it
over the subscription it already holds — no new endpoint, no new credential, no
unauthenticated surface. This file settles *what is in the message* and what the
ingester does with it.

## The message

Sent by the plugin on connect, and on every cluster-state change that alters a
field below.

| Field | Type | Meaning |
|---|---|---|
| `indexUUID` | string | The identity. ⚠️ **Never `indexName`** — a deleted and recreated index reuses the name and must not inherit the old stream's offsets |
| `indexName` | string | For operators and log lines only. Never a routing input |
| `aliases[]` | string[] | Write aliases resolving to this index |
| `numShards` | u16 | Primary shard count = **partition count** |
| `routingNumShards` | u16 | The split-aware denominator (`OperationRouting`) |
| `routingFactor` | u16 | `routingNumShards / numShards` |
| `replicationMode` | enum | `SEGMENT` or `DOCUMENT` |
| `allActive` | bool | Whether every replica ingests |
| `partitioningMode` | enum | `explicit` · `routing_key` · `os_routing` (ADR-0006) |
| `lanes[]` | i8[] | Permitted scheduling buckets (ADR-0014) |
| `ackMode` | enum | Index-level; per-record is unsound (head-of-line blocking) |
| `quorum` | u8 | WAL quorum when `wal=true` (ADR-0013) |
| `registrationEpoch` | u64 | Monotonic per `indexUUID` |

## What the ingester does with it

1. **Last-writer-wins by `registrationEpoch`**, per `indexUUID`. A lower epoch is
   dropped silently — several plugin instances register the same index, and a
   reordered delivery must not resurrect a stale shard count.
2. **Validate, then reject the index rather than guess.** A registration failing
   validation marks the index unroutable and fails its writes with a permanent
   error; it never falls back to a default.
   - `numShards ≥ 1`, `routingNumShards % numShards == 0`
   - `replicationMode == SEGMENT` XOR `allActive` — the strict XOR of ADR-0009
   - `routing_partition_size > 1` without a producer `_id` → **rejected**
     (ADR-0015: the simple hash form does not hold, and a placement error here is
     a silent wrong answer, not an exception)
3. **A changed `numShards` starts a new stream generation.** Offsets are not
   comparable across a shard-count change, so the ingester seals the current
   generation and begins another rather than reusing partition numbers.
4. **Unknown fields are ignored; unknown enum values reject the registration.**
   Adding a field is not a contract change; changing the meaning of one is
   ([Q11](../../research/50-open-questions.md)).

## Cost

⚠️ Registration rate scales with **indices × plugin restarts × cluster-state
changes**, never with records. It carries no object-store request at all: the
ingester holds the table in memory and rebuilds it from registrations after a
restart. Nothing here may add a PUT, a GET or a LIST — see
[cost.md](../standards/cost.md).

## Security

- The message arrives on the **authenticated subscription**; it is never accepted
  from an anonymous caller.
- It carries **no credentials** — non-negotiable, and the reason ADR-0015
  rejected both an unauthenticated plugin endpoint and a producer-held password.
- `indexUUID` is scoped to the subscription's **trust domain**. A plugin may
  register only indices in the cluster it belongs to; segments are never bundled
  across two ([ADR-0010](decisions/0010-multi-tenancy-and-security-model.md)).
- ⚠️ `indexName`, aliases and routing values are **never logged** and never
  become metric labels ([observability.md](../standards/observability.md) rule 1).

## Wire-format status

⚠️ **Not frozen.** No byte layout is fixed here — the table is the field set and
its semantics. The encoding lands with the subscription protocol in M1.11/M1.12,
and from that commit onward every change follows
[`wire-format-change`](../../../.agents/skills/wire-format-change/SKILL.md) and the
N-1 negotiation rule in [build.md](../standards/build.md).
