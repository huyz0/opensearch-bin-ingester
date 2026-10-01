# binstore-backends

Implementations of `BinStore`: in-memory, local filesystem, and later S3, GCS and
Azure. And of `JournalFile` (ADR-0083): `FileJournalFile` on the pod's
`emptyDir`, and `MemoryJournalFile`, the fake that loses unforced bytes on an
injected crash.

**Depends on:** `binstore-spi`.

⚠️ Nothing in `plugin` may depend on this module. Keeping cloud SDKs out of the
OpenSearch JVM is a stated architectural property, not an accident —
[architecture.md](../docs/internal/product/architecture.md) § Dependency rules.
