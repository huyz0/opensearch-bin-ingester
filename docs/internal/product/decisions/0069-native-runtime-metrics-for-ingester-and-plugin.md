# 0069. Use each runtime's native metrics registry

Status: accepted
Date: 2026-09-25
Requirements: FR-10, FR-11, NFR-9
Research: docs/internal/standards/observability.md §1–3

## Context

M8.60 exports diagnostic values from two different processes: the ingester is a
Helidon SE service and the consumer runs inside an OpenSearch node. The values
already exist as in-memory counters and state: fallback tier and entry counts,
poll failures, reconnects, progress-push failures, EndpointSlice watch failures,
and inbox intents whose application attempt failed. The ingester-side stuck
value cannot be discovered by scanning the durable inbox on scrape: that would
add LIST/GET traffic to an idle metrics path and violate cost rules R2 and R3.

The telemetry contract is **15 fixed, process-local measurements**: one current
fallback-tier gauge; four tier-entry counters; one reconnect counter; six poll
failure counters; one progress-push failure counter; one EndpointSlice-watch
failure counter; and one stuck-intent failed-attempt counter. The instruments
have no application-defined tags or labels. Helidon's exposition adds only its
fixed `scope="application"` dimension; the cardinality contribution is fixed
regardless of indices, streams, pods or partitions.

## Decision

Use the host runtime's registry at each process boundary:

* The plugin registers fixed-name counters and a gauge through OpenSearch's
  injected `TelemetryAwarePlugin` `MetricsRegistry`. Existing cumulative
  counters are seeded at registration and subsequent source events increment
  the corresponding metric counter; the seed-and-listener handoff is atomic
  with source updates so registration cannot lose or double-count an event. The
  current worst fallback tier is a gauge. No OpenSearch or Helidon type leaks
  into `client`.
* The ingester registers fixed-name application counters and a gauge in
  Helidon's SE metrics registry and exposes them through Helidon's
  `/observe/metrics` endpoint.
  `InboxDrain` reports the size of each failed per-pod intent batch to an
  assembly-owned in-memory counter. It increments by the batch size for each
  failed intent-application attempt, not a claim about the current number of
  distinct inbox objects; retries of the same intent count again.
* All 15 measurements use fixed names and no application-defined tags. The fallback enum and poll
  failure enum are encoded in a finite set of metric names rather than labels.
  The endpoint reads only in-memory values and is not allowed to perform store
  I/O.

The exact names are `biningester_fallback_current_tier`,
`biningester_fallback_tier_{push,reconnect,poll_chain,recover}_entries_total`,
`biningester_subscription_reconnects_total`,
`biningester_subscription_poll_failure_{unavailable,refused,server_error,unreachable,malformed,callback}_total`,
`biningester_progress_push_failures_total`,
`biningester_endpointslice_watch_failures_total`, and
`biningester_inbox_stuck_intent_attempts_total`. The current-tier gauge reports
the `AutomaticTier` ordinal (0–3); all other instruments are counters seeded
from their source counts and incremented for subsequent source events. No
instrument has a dynamic tag.

OpenSearch 3.8.0's telemetry-aware plugin interface is marked experimental.
This project targets and compiles against 3.8.0; upgrading OpenSearch must
recompile and re-run plugin metric registration tests before release. The
interface is intentionally used only at the plugin boundary, never in the
consumer library.

## Alternatives considered

* **One third-party registry in both processes.** Rejected: it adds a second
  telemetry implementation to both runtimes and another dependency in the
  OpenSearch plugin class loader, while Helidon and OpenSearch already expose
  host registries.
* **A custom `/metrics` encoder or a new cross-module metrics SPI.** Rejected:
  it duplicates exporter semantics and creates a new seam solely to bridge two
  process-local registries. It also makes ownership and lifecycle harder than
  15 fixed measurements.
* **Dynamic labels for tier or failure kind.** Rejected: the observability
  standard reserves `tier` for test metrics and its label allow-list is closed.
  Fixed names preserve the same finite information without multiplying series.
* **Discover stuck intents by scraping the object store.** Rejected: one
  scrape-time LIST/GET would turn monitoring into recurring storage traffic,
  including while the ingester is otherwise idle. Counting the existing failed
  batch path costs no store operation.

## Consequences

Tests must assert all 15 exact names, instrument types, and source values: the
plugin's tier gauge, four tier-entry counters, reconnect counter, six poll
failure counters, and progress-push counter; plus the ingester's EndpointSlice
counter and failed-intent-attempt counter. The failed-intent counter increases
by the failed batch size on each attempt (including retries); it is explicitly
not a gauge of currently outstanding unique inbox objects. Tests must also
use a latch-controlled source update at the counter seed/listener boundary and
prove the host counter contains the seed plus that event exactly once. Tests must also
scrape the assembled ingester metrics endpoint repeatedly and prove its
`CountingBinStore` request counts do not change. Helidon's exposition test pins
its fixed application scope as the only emitted dimension. The existing JVM
metric gate must continue rejecting a per-index label. A configured OpenSearch telemetry
provider is required for the plugin's instruments to be exported; the plugin
itself does not install or configure that provider.

The endpoint path and registry APIs are owned by their respective runtimes.
There is no cross-process aggregation: monitoring scrapes each ingester and
OpenSearch node separately, as it already does for those runtimes.
