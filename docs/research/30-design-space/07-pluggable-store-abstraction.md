# The pluggable object-store SPI

**Status:** proposal · **Confidence:** high (shape validated against AutoMQ's production SPI) ·
**Last updated:** 2026-08-29

**Read this if:** you are defining the store interface or writing a backend.
**One-line takeaway:** keep the interface small — AutoMQ ships production S3 on ~8 methods — but add
the two things they did not need and we cannot live without: **conditional writes** and a
**cost meter**.

---

## 1. The interface

> ⚠️ **REVISED by [ADR-0022](../../internal/product/decisions/0022-store-spi-pages-explicitly.md).**
> Two signatures below are superseded: `list` returns a `ListPage` with an
> explicit `maxKeys`, not a lazy `Stream` — a stream hides paging inside the
> backend and makes LIST requests invisible to the cost meter (R9) — and
> `ObjectStat` carries **no** `lastModifiedMillis`, because no decision here may
> depend on a store's clock. When this section and the ADR disagree, the ADR wins.

```java
public interface BinStore extends Closeable {
    // --- reads -------------------------------------------------------------
    InputStream          get(String key);                              // whole object, streamed
    InputStream          getRange(String key, long start, long endIncl);
    Optional<ObjectStat> stat(String key);                             // size + version, no body

    // --- writes ------------------------------------------------------------
    Version              put(String key, Body body);                   // unconditional
    Optional<Version>    putIfAbsent(String key, Body body);           // empty => already exists
    Optional<Version>    putIfMatch(String key, Body body, Version v); // empty => version moved
    MultipartWriter      multipart(String key);                        // for objects > partSize

    // --- enumeration / lifecycle ------------------------------------------
    Stream<ObjectStat>   list(String prefix, String startAfter);       // lazily paged
    void                 delete(List<String> keys);                    // batched

    Capabilities         capabilities();
}

record ObjectStat(String key, long size, Version version, long lastModifiedMillis) {}
record Version(String token) {}                 // ETag on S3/Azure, generation on GCS
record Capabilities(boolean conditionalWrites, boolean batchDelete,
                    long maxKeyBytes, long minPartSize, CostTable costs) {}
```

**Design rules:**
- **Streaming in and out.** `Body` is a supplier of a stream or a scatter list of buffers, never a
  `byte[]`. `get` returns an `InputStream`, never a materialised array. Constraint C8 is enforced at
  the SPI boundary, not by convention — if the interface cannot express a full-buffer read, nobody
  can accidentally write one.
- **`Version` is opaque.** Never parse it. S3 ETags are not MD5 for multipart objects; GCS uses
  generations, not ETags, for PUT preconditions.
- **Blocking, not `CompletableFuture`.** With virtual threads, blocking calls *are* the concurrent
  API, and they are far easier to reason about. (AutoMQ's SPI is future-based because it predates
  ubiquitous virtual threads — this is a place to deliberately diverge. See
  [java-runtime](../40-implementation/01-java-runtime-helidon-vthreads.md) §3.)
- **`Capabilities` is checked at startup**, so a store lacking conditional writes fails loudly
  rather than silently corrupting the commit log.

## 2. Backends

| Backend | Purpose | Notes |
|---|---|---|
| `S3BinStore` | production | SigV4; `If-None-Match: *` / `If-Match`; handle 409 + 412 with redrive |
| `GcsBinStore` | production | `x-goog-if-generation-match` (**not** `If-Match`) |
| `AzureBlobBinStore` | production | `If-None-Match: *` / `If-Match` on Put Blob |
| `LocalFsBinStore` | dev + the fast test tier | `O_CREAT\|O_EXCL`; CAS via temp file + atomic rename |
| `MemoryBinStore` | unit tests, simulation | required for the failover simulator ([metadata-and-cas](03-metadata-and-cas.md) §7) |
| `FaultyBinStore` | a decorator | injects latency, 5xx, 409/412, reordering, partial reads |
| `CountingBinStore` | a decorator | the cost meter ([cost-model](../00-problem/02-cost-model.md) §7) |

AutoMQ ships exactly this set (`AwsObjectStorage`, `LocalFileObjectStorage`, `MemoryObjectStorage`)
— evidence the split is the right one.

## 2b. MinIO is a test fixture, not a backend

⚠️ **MinIO is never a production target.** It exists to exercise the S3 wire
protocol in a local Docker container without paying AWS for every test run. It is
a **test double for S3**, not an entry in the supported-backend list, and it is
not a separate implementation — it is `S3BinStore` pointed at a different endpoint
via `BucketURI`.

Stating that plainly matters: otherwise "it doesn't work on MinIO" becomes a bug
report, and a week goes into someone else's stability problem.

### What MinIO is good for

The entire non-CAS surface, which is most of the SPI:

- SigV4 signing correctness
- `Range` request handling, including zero-length, past-EOF and last-byte ranges
- Multipart upload flow and abort
- `list` pagination and `startAfter` ordering
- Error mapping — 404, 412, 409 — reaching the right SPI outcome
- Throughput smoke tests and gross regressions

### What it must **not** be trusted for

| | Why |
|---|---|
| **CAS semantics and the commit protocol** | MinIO's conditional writes exist but are **not stable enough to distinguish our bug from theirs**. A flake there costs days and teaches nothing. Run the commit-protocol simulation against `MemoryBinStore` — we control it, so it can be S3-exact *and* inject divergences deliberately |
| **Latency** | Local MinIO answers in microseconds; S3 is ~25 ms TTFB. Anything tuned against it — flush interval, fan-out threshold, lease TTL — is **modelled, not measured** |
| **Cost in dollars** | Prices are S3's. ⚠️ **Request *counts* are backend-independent, so the cost gates are fully meaningful against MinIO** — only the $ conversion is modelled |
| **Durability** | Single-container MinIO has none worth asserting against |

`Capabilities.conditionalWrites` is still probed at startup rather than inferred —
that is correct regardless of which endpoint is behind the SPI, and it is what
keeps a misconfigured production store from failing silently instead of loudly.

## 3. Conformance suite

One test class, run against **every** backend. This is the highest-leverage test in the project,
because CAS semantics are where the providers actually differ and where a subtle divergence
silently breaks the ordering protocol.

- `putIfAbsent` succeeds exactly once under N concurrent writers; all others observe "exists".
- `putIfMatch` succeeds exactly once per version; losers observe "version moved".
- A `Version` returned by a write is accepted by a later `putIfMatch` (round-trip fidelity).
- Read-after-write is visible immediately (S3 has been strongly consistent since Dec 2020; assert it
  rather than assume it, especially for S3-compatible stores).
- `list` is lexicographic, honours `startAfter`, and pages lazily without buffering all keys.
  > ⚠️ **WITHDRAWN by [ADR-0022](../../internal/product/decisions/0022-store-spi-pages-explicitly.md)** — it demanded the very laziness that hid LIST requests from the meter. Replaced by "one call is one page is one request".
- Keys of exactly 1,024 bytes, and every character our key generator can emit, round-trip
  ([bloom-in-key](02-partition-bloom-in-key.md) §7).
- Range reads: zero-length, past-EOF, and last-byte ranges behave identically everywhere.
- `delete` of a non-existent key is not an error.

## 4. Configuration

URI-style, as AutoMQ's `BucketURI` does — one string, easy to put in a K8s secret or an index param:

```
s3://my-bucket/prefix?region=us-east-1&endpoint=https://s3.amazonaws.com&pathStyle=false
gs://my-bucket/prefix
az://my-container/prefix?account=myacct
file:///var/lib/bin-ingester/store
```

Credentials come from the ambient provider chain (IRSA / workload identity / managed identity) or,
in the plugin, from the OpenSearch keystore — **never from the URI and never from index settings**
([plugin-packaging](../20-opensearch/03-plugin-packaging.md) §3).

## 5. Cross-cutting decorators

Compose rather than complicate the backends (AutoMQ does the same with `ProxyWriter`,
`RetryStrategy`, `TrafficRateLimiter`, `S3LatencyCalculator`):

```
CountingBinStore( GoverningBinStore( RetryingBinStore( RateLimitedBinStore( S3BinStore ) ) ) )
```

⚠️ **`GoverningBinStore` refuses; `CountingBinStore` records.** Counting outside
governing means refusals are counted too, which is what makes
`binstore_governor_refusals_total` meaningful. See
[15-cost-governor.md](15-cost-governor.md).

- **Retry** — exponential backoff with full jitter for 5xx/throttling. **409/412 are *not* retried
  here**; they are protocol outcomes that the caller must redrive with fresh state. Getting this
  boundary wrong (blind-retrying a 412) turns a safe protocol into a livelock.
- **Rate limit** — protect against a runaway loop generating a five-figure bill in minutes. A hard
  ceiling on requests/s per pod is cheap insurance; log loudly when it engages.
- **Count** — per-op counters and byte totals, tagged by purpose (`data`, `commit`, `lease`,
  `gc`, `recovery`) so the cost meter can attribute spend to a subsystem.
- **Latency tracking** — per-op histograms; feeds the adaptive flush sizing and alerting.

## 6. Open questions

- Do we need `putIfMatch` at all if the commit chain is write-once? (Yes — leases and the ordinal
  registry need it. But it is worth checking whether those two can also be expressed as write-once
  sequences, which would let `Capabilities` require only `putIfAbsent` and widen backend support to
  stores with weaker CAS.)
- Should the plugin use the same SPI, or a read-only subset (`get`, `getRange`, `list`, `stat`)?
  A read-only subinterface keeps the plugin's dependency surface minimal
  ([plugin-packaging](../20-opensearch/03-plugin-packaging.md) §2). **Leaning yes: `BinStoreReader`.**
- Is a CRT/async client ever worth it for the ingester's write path, or do virtual threads plus
  enough concurrent blocking PUTs saturate the NIC? **Benchmark** —
  [benchmarking-plan](../40-implementation/03-benchmarking-plan.md) §4.
