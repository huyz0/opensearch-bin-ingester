# 0072. Attribute low-rate lease PUT cost without excluding it

Status: accepted
Date: 2026-09-26
Requirements: NFR-1
Research: docs/research/00-problem/02-cost-model.md

## Context

ADR-0062 applied its low-rate ceiling to every PUT observed by the assembled
node. The first full RustFS profile measured 228 aggregate PUTs over
300.2968 seconds at a 5-second ingest interval ceiling, exceeding its
124-request ceiling. That counter is the correct total-cost view, but it
combines segment and commit writes with lease renewals. The configured lease
renew cadence was 3 seconds; those control-plane writes are not extra flushes,
yet are real billed object-store requests. Hiding them would understate cost;
counting them as flushes would test the wrong behavior.

The final full size-triggered profile passed all three five-minute points:
40 MiB/s measured 2,515 PUTs and 0.24935513908835705 requests/MiB; 80 MiB/s
measured 2,509 PUTs and 0.24924154530852677 requests/MiB; 160 MiB/s measured
2,323 PUTs and 0.2500920594047511 requests/MiB. Each point recorded zero LISTs.
These results retain the existing `< 0.30` aggregate budget.

The final full-profile purpose-aware low-rate run measured 1,980 aggregate
PUTs at a 250 ms ceiling (935 data, 935 commit, 10 checkpoint, 99 lease,
1 other) and 232 at a 5 s ceiling (60 data, 60 commit, 10 checkpoint, 101 lease,
1 other). Both points recorded zero LISTs. Data plus commit delta remained
within two PUTs per interval ceiling; checkpoint pairs stayed within their
one-minute cadence and lease PUTs within their three-second renewal cadence.
The categories reconcile exactly to aggregate PUTs.

## Decision

Keep the size-triggered `< 0.30 aggregate write requests per MiB` bound.
At low rates, assert at most two segment-data plus commit-delta PUTs per
pod per interval ceiling. Attribute checkpoint pairs and lease PUTs
independently and assert their counts do not exceed their configured cadence.
Classify every other PUT as `other`. The opt-in macro snapshot exposes five
additive purpose counts;
their sum must equal aggregate `puts`, which remains the reported and costed
total. LISTs remain zero for these write paths.

Checkpoint objects are also distinct from commit deltas: each dirty checkpoint
event writes one immutable checkpoint and its latest-pointer object, at the
shipped one-minute time trigger (or the configured delta-count trigger). Their
two PUTs are cadence-bounded independently and remain in aggregate cost.

The full-profile low-rate measurements and final purpose counts are recorded in
M9.56 and `measurements/results/low-rate.csv` in this decision's implementation
commit.

## Alternatives considered

- **Keep the aggregate 124-PUT low-rate ceiling.** Rejected: the 228 observed
  requests include lease renewals at a configured 3-second cadence, so this
  ceiling treats lease maintenance as extra flushes and would fail correct
  data/commit behavior.
- **Exclude lease PUTs from the reported total.** Rejected: each conditional
  lease write is a real object-store request and contributes to the bill. It
  must remain in aggregate counters and cost results.
- **Raise the aggregate low-rate threshold to 228 or more.** Rejected: that
  weakens a cost gate without separating workload writes from lease cadence;
  it could mask an actual excess flush rate.
- **Keep checkpoint PUTs inside the data-plus-commit flush bound.** Rejected:
  checkpoints are recovery metadata written by a separate dirty/time policy;
  charging those periodic two-object events to every flush interval confounds
  the policy being tested. Their configured cadence is independently bounded,
  and both PUTs remain visible in aggregate totals.

## Consequences

The macro-count JSON gains five additive PUT-purpose fields, while its existing
aggregate counters and legacy untracked response remain compatible. Object-key
purpose attribution is bounded to data, commit delta, checkpoint, lease, and other;
unknown paths are conservatively retained in `other`. NFR-1's high-rate total
cost remains unchanged. Low-rate reports show aggregate PUTs alongside the
asserted data-plus-commit flush bound and lease cadence, so neither cost nor
behavior is hidden.
