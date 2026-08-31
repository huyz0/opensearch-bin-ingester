# 0019. Two delivery surfaces: an in-process API and an HTTP API

Status: accepted
Date: 2026-08-30
Requirements: FR-1, FR-4, NFR-6
Research: docs/research/30-design-space/14-producer-contract.md

## Context

Records reach the writer two ways, and both are deliverables:

1. **In-process** — a host process (a *gateway*: producer logic plus the ingest
   library in one shell, ~2 per AZ) calls a Java API directly. No HTTP, no
   serialisation, no `_bulk` parse.
2. **Over HTTP** — a remote producer POSTs `_bulk`, as
   [14-producer-contract.md](../../../research/30-design-space/14-producer-contract.md)
   specifies.

## Decision

**The ingest core is a library. The HTTP endpoint is a thin adapter over it.**

```
gateway process                    remote producer
  ├── producer logic                    │ POST /v1/bulk
  └── ingest library  ◀─────────────────┴── HTTP adapter (Helidon)
        ├── accumulators                       (parses, then calls the same API)
        ├── writer  ──▶ object store
        ├── reader  ──▶ serves consumers
        └── sequencer
```

⚠️ **One implementation, adapters on top** — the same principle that keeps
`.claude/` a thin adapter over `.agents/skills/`. An HTTP handler containing
ingest logic is a fork waiting to drift, and it would be the fork that gets
tested less.

### Module rule

**No module below the adapter may reference HTTP.** `format`, `binstore-spi`,
`sequencer` and the ingest core compile without Helidon on the classpath, and a
gate should assert it once the build exists. This is the same shape as the
sans-I/O rule: if the core needs a socket to test, it is in the wrong layer.

### The in-process API is a public contract

Not "whatever the HTTP handler happens to call". Sketch:

```java
public interface Ingest extends AutoCloseable {
    /** Blocks per the index's ack_mode. Consumes lazily -- a large batch is never materialised. */
    AppendResult append(IndexRef index, RecordStream records) throws IngestException;

    /** Force a segment cut without waiting for the interval. */
    void flush();
}
```

- **Blocking, not `CompletableFuture`** — with virtual threads, blocking *is* the
  concurrent API ([java-style.md](../../standards/java-style.md) rule 2), and
  concurrency comes from calling it on many virtual threads.
- **`RecordStream` is consumed lazily**, so an in-process caller cannot defeat
  constraint C8 by handing over a materialised list.
- **The durability boundary is the return of `append`**, per the index's
  `wal`/`wal_quorum` settings — identical semantics to the HTTP `202`, because it
  is the same code.
- Backpressure in-process is a **blocking buffer-pool acquire**, not a `429`. Same
  mechanism, different expression.

## Alternatives considered

- **HTTP only, with the gateway calling itself over loopback.** Rejected: it pays
  serialisation, the newline scan and the action-line parse (~5% of a core at
  100 MiB/s) to reach code already in the same JVM, and it makes the fast path
  depend on a socket.
- **In-process only**, no HTTP. Rejected: it forfeits the drop-in `_bulk` claim,
  and any producer not written in Java has no path in.
- **Two implementations tuned separately.** Rejected on the fork argument above.

## Consequences

- The HTTP layer becomes small enough to test thinly — parse, map errors, delegate
  — while the ingest core carries the T0/T1 test weight. **That is cheaper to test
  and cheaper to reach 95% on**, since most behaviour is reachable without a server.
- The in-process API is a **published Java contract** and gains a compatibility
  obligation: additive changes only within a major version. It is not covered by
  [`wire-format-change`](../../../../.agents/skills/wire-format-change/SKILL.md) —
  that skill governs bytes that outlive a process — but it needs the same care.
- ⚠️ **A gateway embedding the library holds object-store credentials and buffered,
  not-yet-durable records.** Acceptable because the gateway is a dedicated tier we
  deploy (~6 pods), not an arbitrary application. **If the library is ever embedded
  in a general application, revisit** — the PUT floor tracks writer count, and at
  50+ writers bundling collapses (see the gateway-count analysis on 2026-08-30).
- M1 builds the in-process API first and the HTTP adapter second, because the
  skeleton's four risky assumptions are all on the consumer side and none of them
  need HTTP to expose.
