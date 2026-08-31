# 0017. Every pod writes; the adaptive interval is the dial; an operator sizes the fleet

Status: accepted
Date: 2026-08-30
Requirements: NFR-1, FR-12
Supersedes: [0016](0016-designated-writer-per-az.md)
Research: docs/research/30-design-space/13-worked-example-multi-tenant.md §8, §10

## Context

[ADR-0016](0016-designated-writer-per-az.md) introduced a **designated writer per
AZ**: pods forward their buffers intra-AZ to one writer, which builds and PUTs the
segment. It was justified by the collapse of PUT cost at low throughput — $311/month
to move 2.5 TB.

Decomposing that saving shows the justification was misattributed:

| Change | Objects/s @ 1 MiB/s | Cost | Share of the saving |
|---|---|---|---|
| 6 writers @ 250 ms (original) | 24.00 | $311.04/mo | — |
| **6 writers @ 5 s** (adaptive interval only) | 1.20 | **$15.55/mo** | **95.0%** |
| 3 writers @ 5 s (+ forwarding) | 0.60 | $7.78/mo | 2.5% |

**The adaptive interval did 95% of the work. Forwarding added $7.78/month** — and
the interval ceiling is a stronger dial than the writer count at every setting:

| Interval ceiling | 6 writers | 3 writers | Forwarding saves |
|---|---|---|---|
| 1 s | $77.76/mo | $38.88/mo | $38.88/mo |
| **5 s** | **$15.55/mo** | $7.78/mo | **$7.78/mo** |
| 30 s | $2.59/mo | $1.62/mo | $0.97/mo |

Against that: an extra network hop, a writer lease, a new failure mode, a
chokepoint at the moment traffic ramps (before scale-up detects), and an
architecture unfamiliar to anyone operating it.

## Decision

**1. Every pod writes its own segments. There is no forwarding and no writer
lease.** A pod that accepts records builds and PUTs them itself.

**2. The adaptive flush interval is the cost dial**, and it needs **no
coordination at all**: each pod measures `fillRatio` on its own flushes and
lengthens or shortens its own interval. No forward-ack, no shared signal, no lease.

**3. Fleet size is a Kubernetes concern.** An operator (or HPA on a custom metric)
scales pod count, bounded below by the HA floor of 2 per AZ. Scale on **ingest
bytes/s per pod** as the primary signal, with the `429` rate and p99 ack latency as
guardrails — **not CPU**, which is misleading for an I/O-bound service that is
mostly idle.

**4. One dial propagates, and operators should see it as one:**

```
latency SLO  ->  maxStreamLatency  ->  effective segment interval  ->  PUT cost
                                       = min(intervalCeiling, tightest run deadline)
```

A segment cannot wait longer than the tightest deadline of any run inside it, so
the lane deadlines already bound the interval. There is no separate knob to get
wrong.

**5. Fleet sizing guideline** (advice to the operator, not a runtime constraint):
`pods ≈ clamp(HA floor, bytes × intervalCeiling ÷ segmentSize)`. At thin traffic
the HA floor wins and segments are under-full — **that is accepted**, and it costs
$15.55/month at a 5 s ceiling.

## Alternatives considered

- **Designated writer per AZ with intra-AZ forwarding** (ADR-0016). Rejected on the
  decomposition above: it captures 2.5% of a saving the interval already made, and
  buys a hop, a lease, a failure mode and a ramp-time chokepoint for it.
  ⚠️ Kept as a superseded ADR rather than deleted, because *why it looked
  attractive* is the instructive part: the original $311/month figure was real, and
  attributing it to writer count rather than to flush interval is an easy and
  expensive mistake to repeat.
- **Scale pods to zero at idle.** Rejected: the HA floor is a durability and
  availability requirement, not a capacity one, and $15.55/month does not justify
  losing it.
- **Scale on CPU.** Rejected: the ingester is I/O-bound and mostly idle; CPU would
  under-provision through a ramp and over-provision during compression bursts.

## Consequences

- **Simpler:** no forwarding path, no writer lease, no consistent hash over
  writers, no ramp-time chokepoint, and one fewer thing to explain to an operator.
- The `fillRatio` control loop survives and gets **simpler still** — it is now
  purely local, because it drives one pod's own interval rather than a
  cluster-wide writer count. Its scale-up/scale-down asymmetry and hysteresis
  still apply to the interval.
- Cost at the HA floor is **$15.55/month at a 5 s ceiling** rather than $7.78.
  Accepted deliberately.
- ⚠️ **Two corrections to one ADR in a single session is a signal.** ADR-0016 was
  reasoned from an arithmetic result without decomposing which term produced it.
  The lesson for the [`adr`](../../../../.agents/skills/adr/SKILL.md) skill's
  "quantify" rule: quantify the *alternative* too, not only the problem.
