# 0005. No consumer offset store; OpenSearch owns the position

Status: accepted
Date: 2026-08-30
Requirements: FR-9, NFR-13
Research: docs/research/30-design-space/09-consumer-position-and-watermarks.md

## Context

Kafka stores committed offsets server-side because its consumers are anonymous,
interchangeable group members that rebalance. The obvious move is to mirror that.

OpenSearch already persists the position: `IngestionEngine` writes
`StreamPoller.BATCH_START` into the **same Lucene commit** as the documents.

## Decision

**No ingester-side offset store.** The subscription is client-driven: the plugin
sends `fromOffset`, the ingester holds no cursor.

A position feed exists for exactly one purpose — a **conservative GC watermark**
— piggybacked on the subscription. It may only **extend** retention, never
shorten it below the time floor, and the retention rule takes `min()` across all
shard copies while treating silence as frozen progress rather than as absence.

## Alternatives considered

- **A Kafka-style offset store in the ingester.** Rejected: it is a weaker,
  asynchronous copy that can disagree with what is actually indexed, and when
  they disagree the Lucene commit is right and ours causes data loss. It also
  reproduces `enable.auto.commit`, whose decoupling of commit from processing is
  the classic source of loss and duplication — a bug OpenSearch's design does not
  have.
- **Polling `GetIngestionStateAction` from the ingester.** Rejected: it needs
  OpenSearch credentials and cluster knowledge in the ingester, and returns the
  same in-memory value anyway.
- **Time-based retention only, no watermark.** Rejected: that makes retention a
  data-loss mechanism for a lagging or restarting consumer.

## Consequences

- The ingester stays genuinely stateless with respect to consumers.
- ⚠️ **Watermarks are optimistic.** `getIngestionState()` reports
  `streamPoller.getBatchStartPointer()` — the in-memory pointer — while only
  `lastCommittedBatchStartPointer` survives a crash, and OpenSearch does not
  expose it. Hence the extend-only rule and a `safetyMargin` sized above the
  observed Lucene commit interval.
- A paused shard would pin retention forever, so a `maxRetention` ceiling exists
  and must **alarm** rather than delete quietly.
