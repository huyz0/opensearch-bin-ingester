# 0020. Record envelope carries id/op/version; the consumer assembles the mapper's JSON

Status: accepted
Date: 2026-08-30
Requirements: FR-1, FR-3, NFR-11
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §2

## Context

Producers send **index and delete** mutations with a **monotonic external
version** from the source system. So the record envelope must carry `_id`,
`_op_type` and `_version`, not just a payload.

⚠️ **`IngestionMessageMapper.MapperType` is a closed enum** — `DEFAULT`,
`RAW_PAYLOAD`, `FIELD_MAPPING`. A plugin cannot register its own, so whatever we
store must, when handed to `Message.getPayload()`, already match one of those
three shapes.

- `RAW_PAYLOAD` auto-generates `_id` as `shardId + "-" + pointer` and forces
  `_op_type: index`. **No deletes.** Disqualified.
- `FIELD_MAPPING` lifts `_id` / `_version` / `_op_type` from *fields inside the
  document*. Our producer sends them in the bulk **action line**, not the body, so
  they are not there. Disqualified without asking producers to duplicate them into
  every document.
- `DEFAULT` expects `{"_id":…, "_op_type":…, "_version":…, "_source":{…}}`.

## Decision

**Store compactly; assemble the mapper's JSON in the consumer.**

The segment's per-record framing carries the fields natively:

```
record := [u8 flags][uvarint idLen][id bytes][uvarint version]?[uvarint payloadLen][payload]
          flags: opType (index|create|delete), version-present
```

The **consumer** wraps them into the `DEFAULT` envelope when it materialises the
`byte[]` that `Message.getPayload()` must return — a byte concatenation around an
opaque payload, **never a parse of the document**.

Index setting: `index.ingestion_source.mapper_type: default`.

### Why assemble at read time rather than write time

- The ~70-byte JSON wrapper **never reaches the object store**: no storage cost, no
  bandwidth cost, no compression penalty on 10,000 indices' worth of segments.
- The consumer must allocate a `byte[]` per record anyway (the SPI demands it), so
  the wrap is folded into an allocation that already exists.
- A delete carries no payload, so at write time the envelope would be mostly
  overhead.

## Alternatives considered

- **Store the JSON envelope in the segment.** Rejected: it pays wrapper bytes in
  storage, bandwidth and compression for every record, forever, to save a
  concatenation the consumer performs anyway.
- **`FIELD_MAPPING` with producers duplicating `_id`/`_version` into the document
  body.** Rejected: it pollutes `_source` with routing metadata, makes the
  producer contract leak our implementation, and the mapper's field-removal
  behaviour becomes load-bearing.
- **`RAW_PAYLOAD`.** Rejected: no deletes.

## Consequences

- ⚠️ **A serialise-then-parse round trip per record**: the consumer builds JSON
  that `DefaultIngestionMessageMapper` immediately parses. Trivial at 1,000 rec/s;
  expect roughly **10–20% of a core at 100,000 rec/s**. **Measure it at M9** —
  this figure is modelled.
- **Upstream opportunity, worth recording:** a plugin-extensible
  `IngestionMessageMapper` would let us hand records over without the round trip.
  The enum being closed is the only reason it exists. A candidate contribution to
  OpenSearch, not a blocker.
- ⚠️ **The backfill must carry the source record's own version, in the same
  version space as live traffic** (confirmed 2026-08-30). That is what lets a
  re-bootstrap in lane `-1` run **concurrently with live writes for the same
  tenant, with no coordination**: OpenSearch rejects the older value, so a
  replayed v1 cannot overwrite a live v50.

  **The failure mode if this ever changes is silent.** A backfill stamping
  *replay-time* versions would carry **higher** versions than live traffic, so it
  would overwrite newer data and OpenSearch would accept it as correct —
  external versioning doing exactly what it was told. Nothing in this design
  catches it. It is a **producer-side invariant**, and it is pinned by a test
  rather than by a comment.
- External versioning is now **load-bearing for correctness**, not optional: it is
  what makes at-least-once replay safe on mutations — a redelivered stale delete
  cannot resurrect a document — and what makes cross-lane mutations safe
  ([ADR-0014](0014-priority-lanes.md)). The producer contract must state that a
  missing version downgrades both guarantees.
- The frame gains `_id`, `_op_type` and `_version`, so this is a
  [`wire-format-change`](../../../../.agents/skills/wire-format-change/SKILL.md)
  and lands in M1 with golden files, before anything depends on the old shape.
