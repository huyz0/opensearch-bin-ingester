# binstore-backends

Implementations of `BinStore`: in-memory, local filesystem, and later S3, GCS and
Azure.

**Depends on:** `binstore-spi`.

⚠️ Nothing in `plugin` may depend on this module. Keeping cloud SDKs out of the
OpenSearch JVM is a stated architectural property, not an accident —
[architecture.md](../docs/internal/product/architecture.md) § Dependency rules.
