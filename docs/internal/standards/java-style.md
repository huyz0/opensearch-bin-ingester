# Java style

**Family:** Code
**Read when:** Naming things, choosing a concurrency construct, writing buffer-handling code, or when a diff is hard to read for reasons code-structure.md does not cover.

Rationale:
[`docs/research/40-implementation/01-java-runtime-helidon-vthreads.md`](../../research/40-implementation/01-java-runtime-helidon-vthreads.md).

1. **JDK 25 LTS.** JEP 491 removed `synchronized` pinning in JDK 24; JDK 21 is
   the wrong target for a virtual-thread-heavy service.
2. **Blocking APIs, not `CompletableFuture`.** With virtual threads, blocking
   calls *are* the concurrent API.
3. **CPU-bound work never runs on a virtual thread.** Compression, checksums and
   serialisation go to a bounded platform pool sized to cores.
4. **Never pool virtual threads.** One per task.
5. **No `ThreadLocal` buffer caches.** With millions of short-lived virtual
   threads they are either useless or an unbounded leak. Use an explicit bounded
   pool; use `ScopedValue` for request context.
6. **`ReentrantLock` where a lock is held across I/O** — for `tryLock` with a
   timeout, not for pinning.
7. **Every queue, pool and cache is bounded**, and the bound is configuration,
   not a constant buried in a constructor.
8. **Never materialise a request body or a whole object.** Streams in, streams
   out, at the SPI boundary so it cannot be done by accident.
9. **`byte[]` per record only where the OpenSearch SPI demands it**, and nowhere
   else on the hot path.
10. **`-XX:MaxDirectMemorySize` is set explicitly** wherever direct buffers are
    used; an unset limit turns a leak into a mysterious container OOM.
11. **Records for data, sealed interfaces for closed sets**, no getters-and-
    setters beans.
12. **`Optional` for return values, never for fields or parameters.**
13. **Exceptions carry what an operator needs to act**, never a payload.
