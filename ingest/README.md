# ingest

The library: accumulators, the flush trigger, the writer, the reader, subscription
fan-out and GC. Everything an ingester node does, expressed with no HTTP on the
classpath.

**Depends on:** `binstore-spi`, `format`, `sequencer`.

⚠️ **No HTTP dependency, ever.** Two delivery surfaces share one implementation
([ADR-0019](../docs/internal/product/decisions/0019-two-delivery-surfaces.md)):
the in-process API is this module, and `http` is a thin adapter over it. If a
routing or flush decision can be made in `http`, it is in the wrong module.
