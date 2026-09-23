# 0067. Keep direct-mode fan-out threshold at the cost-parity boundary

Status: accepted
Date: 2026-09-23
Requirements: FR-6, NFR-4
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md#4-when-to-redirect-the-fan-out-rule; docs/research/00-problem/02-cost-model.md#1-unit-prices-aws-us-east-1-s3-standard-aug-2026

## Context

The shipped default direct fan-out threshold is 1, but M3 deferred its basis.
The RustFS measurement in [m9.14-direct-threshold.md](../measurements/m9.14-direct-threshold.md)
observed 1 proxy GET versus N direct GETs for fan-outs 1–16, and respectively
N × 65,536 versus 0 segment bytes through the ingester. The RustFS run measured
no cloud S3 TTFB variance. The cost table prices AWS Standard GET at $0.0004
per 1,000 and same-region S3-to-EC2 bytes at zero.

## Decision

Keep `directFanOutThreshold` at **1** when direct mode is enabled. At fan-out 1,
proxy and direct each cost one GET ($0.0000004 per segment); direct is request-
cost neutral and removes the segment payload from the ingester stream. Above
fan-out 1, direct's request count grows linearly while proxy stays at one: at
fan-out 16 the costs are $0.0000064 versus $0.0000004 per segment. The default
is therefore the largest cost-only threshold that does not increase billable
GETs relative to proxy.

This is a request-cost parity decision, not a claim that direct is cheaper in
all resource dimensions. The local CPU samples were coarse and did not prove a
CPU saving; the cloud latency half is NOT-RUN. No code default changed.

## Alternatives considered

- **Threshold 0 (never direct by the fan-out rule):** avoids signing and makes
  proxy the tie-break at fan-out 1, but gives up direct's zero ingester segment
  bytes without reducing a billable GET. Rejected in favor of the existing
  cost-parity point, not on a claim of lower total dollars.
- **Threshold above 1:** rejected because each extra direct consumer adds one
  GET. At 16 consumers, the measured comparison is 16 GETs versus 1; same-region
  byte transfer is free and cannot recover the additional request charge.
- **Choose the threshold from S3 TTFB or latency:** not available on this rig.
  RustFS request counts are valid, but its latency is not an AWS S3 observation.

## Consequences

- The fan-out threshold remains configuration with a documented default of 1.
- A future cloud run that records real S3 TTFB distributions may motivate
  paying for additional direct GETs to meet a latency objective. Such a change
  must state the added request cost per segment and amend this ADR; it cannot
  silently convert the local-rig result into an S3 latency claim.
- M9.15's curve must label GET counts as observed, dollars as modelled from the
  stated price table, local CPU as a coarse rig observation, and cloud S3 TTFB
  as NOT-RUN.
