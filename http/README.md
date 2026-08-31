# http

The Helidon adapter. Parses `_bulk` as a stream, maps errors to status codes, and
delegates to `ingest`. It owns no decision of its own.

**Depends on:** `ingest`.

Nothing depends on `http`
([architecture.md](../docs/internal/product/architecture.md) § Dependency rules,
rule 4). It is a front door, and a front door that something else depends on has
stopped being one.
