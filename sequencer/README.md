# sequencer

Leases, epoch fencing, the write-once commit log, and checkpoints. The coordination
layer, built on compare-and-swap against the object store rather than on a
consensus cluster ([ADR-0011](../docs/internal/product/decisions/0011-no-consensus-cluster.md)).

**Depends on:** `binstore-spi`, `format`.

Invariants I1-I5 in [architecture.md](../docs/internal/product/architecture.md)
are this module's obligation. They are tested by deterministic simulation, not
by hoping.
