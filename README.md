# opensearch-bin-ingester

Object-store ingestion for OpenSearch pull-based indexing. Bundles writes from
many indices and partitions into few objects, at roughly 1/40th the marginal cost
of Kafka in the same path, trading seconds of latency for it.

**Agents and contributors start at [AGENTS.md](AGENTS.md).** Research that
predates the code is in [docs/research/](docs/research/README.md).

Status: research complete, harness complete, no product code yet. See
[roadmap.md](docs/internal/product/roadmap.md).

## Licence

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
