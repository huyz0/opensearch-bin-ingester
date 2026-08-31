# plugin

The OpenSearch `IngestionConsumerPlugin` implementation: the factory, the shard
consumer, the pointer type and the message type.

**Depends on:** `client`, and through it `format`. Never on `binstore-backends`.

Built against the OpenSearch 3.8 pull-based ingestion SPI —
[docs/research/20-opensearch/01-pull-based-ingestion-spi.md](../docs/research/20-opensearch/01-pull-based-ingestion-spi.md).
