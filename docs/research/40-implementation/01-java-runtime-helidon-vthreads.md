# Java runtime: Helidon SE 4, JDK 25, virtual threads

**Status:** proposal · **Confidence:** high for the JDK/virtual-thread facts, medium for specific
Helidon API details (verify against the version you pin) · **Last updated:** 2026-08-29

**Read this if:** you are setting up the ingester skeleton or making a concurrency decision.
**One-line takeaway:** target **JDK 25 LTS** (not 21) — JEP 491 removed `synchronized` pinning in
JDK 24, which is the difference between virtual threads being safe by default and being a footgun.
Then write plain blocking code and let one virtual thread per request do the work.

---

## 1. Versions

| Component | Version | Why |
|---|---|---|
| **JDK** | **25 LTS** | First LTS containing **JEP 491** (virtual threads no longer pinned by `synchronized`) and finalised Scoped Values. JDK 21 LTS is the wrong choice for a virtual-thread-heavy service |
| **Helidon SE** | 4.x (4.5.x at time of writing) | The Níma web server, rewritten for virtual threads. Blocking thread-per-request programming model with reactive-grade throughput. Requires JDK 21+ |
| Build | Gradle or Maven | The plugin side must integrate with OpenSearch's Gradle build; the ingester is free to choose |

### Why JEP 491 matters concretely
Before JDK 24, a virtual thread blocking inside a `synchronized` block **pinned** its carrier
platform thread. With a default carrier pool sized to CPU count, a handful of pinned threads
deadlocks the whole server. Since JDK 24 the monitor is associated with the virtual thread rather
than the carrier, so `synchronized` code (including code deep inside third-party libraries and the
JDK) no longer pins. Native frames and a few corner cases still pin, but the common footgun is gone.

**Practical consequence:** we can use blocking libraries — including the **synchronous** AWS SDK
client — without auditing every dependency for `synchronized`.

## 2. Helidon SE shape

```java
WebServer.builder()
    .port(8080)
    .routing(r -> r
        .post("/v1/ingest",    IngestHandler::handle)
        .post("/v1/subscribe", SubscribeHandler::handle)   // long-lived streaming response
        .get ("/v1/health",    HealthHandler::handle))
    .build()
    .start();
```

Key APIs (verify signatures against the pinned version):
- **Request body as a stream:** `req.content().inputStream()` — the `ReadableEntity` abstraction.
  **Never `req.content().as(String.class)` or `as(byte[].class)`** on the ingest path: those
  materialise the whole body and defeat constraint C8.
- **Response body as a stream:** `res.outputStream()`. Headers must be set *before* calling it.
  Used for the long-lived subscribe response. Note that `Content-Length` cannot be set for a
  streamed response, and gzip encoding strips it — fine for an event stream.
- **HTTP/2** is enabled when `helidon-webserver-http2` is on the classpath. We want it for the
  subscribe channel (multiplexing, flow control).
- **Config:** `max-concurrent-requests` bounds active virtual threads;
  `idle-connection-timeout` defaults to 5 minutes — **raise it well beyond the subscription
  heartbeat interval** or long-lived subscribers get dropped, which is an easy and confusing bug.
- **`max-payload-size`** must be set deliberately: `-1` (unlimited) plus streaming is correct for
  ingest; a finite cap plus streaming is safer.

## 3. Concurrency model

| Work | Threading | Rationale |
|---|---|---|
| HTTP request handling | virtual thread per request (Helidon default) | I/O-bound, high count |
| Per-`(index,partition)` buffer accumulation | lock-striped, or one owner per stream | Contention is the enemy; stripe by `streamId.hashCode()` |
| Flush / segment build | a small **platform** thread pool | CPU-bound (compression, CRC) — virtual threads give nothing here and hide the parallelism limit |
| Object-store PUT/GET | virtual threads, blocking SDK calls | The main win: hundreds of concurrent in-flight requests with no async plumbing |
| Sequencer | single owner thread per slot group | Ordering is inherently serial; do not parallelise it |
| Subscription fan-out | virtual thread per subscriber, bounded queue | Slow subscribers must not block the sequencer |

**Rules:**
- **CPU-bound work never runs on a virtual thread.** Compression, CRC and serialisation go to a
  bounded platform pool sized to available cores. Virtual threads do not add CPU.
- **`ThreadLocal` buffer caches are an anti-pattern here.** With millions of short-lived virtual
  threads, a per-thread cached buffer is either useless or an unbounded leak. Use an **explicit
  bounded pool** with acquire/release, and consider `ScopedValue` (finalised in JDK 25) for
  request-scoped context instead of `ThreadLocal`.
- **Do not pool virtual threads.** Create one per task. Pooling them reintroduces the limit they
  exist to remove.
- Prefer `ReentrantLock` over `synchronized` where a lock is held across I/O — not for pinning any
  more, but because it supports `tryLock` with a timeout, which is what you want when the object
  store is slow.

## 4. Structured concurrency — ✅ do not build on it (Q14)

Structured concurrency would be a natural fit for "PUT the segment and commit to N sequencers,
cancel everything if one fails". **It is not ready.**

- JEP 505 is the **fifth** preview, in JDK 25.
- JEP 525 is the **sixth** preview, in JDK 26.
- Finalisation is expected in **JDK 27**.

Keep fan-out in an explicit `ExecutorService` and revisit at JDK 27.

⚠️ The API was **reshaped** in the fifth preview — a `StructuredTaskScope` is now opened by a static
factory and its policy chosen by passing a `Joiner`, rather than by subclassing. That churn is
exactly why building the architecture on a preview API would have cost a rewrite, and it is the
general rule: **no preview API on a load-bearing path.**

## 5. Observability

- **Metrics:** Helidon's metrics support, plus our own cost counters
  ([cost-model](../00-problem/02-cost-model.md) §7). The essential dashboard: requests/MiB by
  operation, flush size histogram, commit latency, lease epoch changes, subscriber lag,
  **idle request rate (must be zero)**.
- **Latency:** HdrHistogram for end-to-end and per-stage. Report p50/p99/p99.9 — object-store tail
  latency is the interesting part and means are useless for it.
- **Tracing:** OpenTelemetry, sampled. A trace should span producer → buffer → PUT → commit → push →
  consumer GET; that single trace is what makes latency regressions diagnosable.
- **JFR** in production at low overhead; it is the fastest way to find allocation hot spots and
  pinning events (`jdk.VirtualThreadPinned`).

## 6. Things that will bite

| Trap | Symptom | Avoidance |
|---|---|---|
| `idle-connection-timeout` (5 min default) | subscribers silently dropped | raise it; heartbeat well inside it |
| Materialising the request body | heap spikes, OOM under large batches | stream; assert in tests with a huge body and a small heap |
| Unbounded event queues to subscribers | one slow consumer OOMs the pod | bounded queue + degrade to `tail` events |
| `ThreadLocal` buffers with virtual threads | memory growth proportional to request count | explicit pool |
| Blind-retrying a 412 | livelock in the commit protocol | 409/412 are protocol outcomes, redrive with fresh state ([store SPI](../30-design-space/07-pluggable-store-abstraction.md) §5) |
| Sync SDK + small connection pool | throughput collapse under virtual threads | size the HTTP connection pool to expected concurrency, not to core count |
| Direct `ByteBuffer` leaks | native OOM, invisible in heap dumps | explicit pool with accounting; `-XX:MaxDirectMemorySize` set deliberately |

That second-to-last row deserves emphasis: it is the most common way virtual-thread migrations fail.
The apparent concurrency limit moves from the thread pool to the connection pool, and the symptom
looks like "virtual threads didn't help."

---

**Sources:** [JEP 491](https://openjdk.org/jeps/491) ·
[InfoQ: Virtual Threads after JDK 24](https://www.infoq.com/articles/virtual-threads-after-jdk24/) ·
[Helidon 4 WebServer docs](https://helidon.io/docs/v4/se/webserver) ·
[Helidon Níma](https://medium.com/helidon/helidon-n%C3%ADma-helidon-on-virtual-threads-130bb2ea2088)
