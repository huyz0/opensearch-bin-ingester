# binstore-spi

The object-store seam. `BinStore`, `Version`, `Capabilities`, and the decorators
that wrap any backend: retry, rate-limit, and **counting**. Also `JournalFile`,
the fast journal's one pod-local file (ADR-0083): here because this is the
dependency-free module the I/O-exempt adapters in `binstore-backends`
implement against.

**Depends on:** nothing. That is deliberate — it is the one interface both the
ingester and the conformance suite compile against, and a dependency here would
be inherited by every module in the build.

The counting decorator is why this module exists as a seam rather than as an
S3 client: every object-store request in the project passes through one type, so
`docs/internal/standards/cost.md` can be asserted in a test rather than reviewed
by eye.
