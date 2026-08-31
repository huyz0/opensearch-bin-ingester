# Mission

Replace Kafka in the OpenSearch pull-based ingestion path with a stateless
service that writes bundled objects to a pluggable object store, at roughly
**1/40th of the marginal cost**, trading seconds of latency for it.

## What it is

- An **ingester** (Java 25, Helidon SE 4, virtual threads) that accepts
  writes for many indices, bundles them into few objects, assigns stable
  per-partition offsets, and pushes tail notifications.
- An **OpenSearch plugin** implementing `IngestionConsumerPlugin`.
- The **object store is the only durable dependency** — and the coordination
  substrate, via compare-and-swap.

## What it must never become

- **A Kafka implementation.** No consumer groups, no rebalancing, no wire
  compatibility. The only consumer is our plugin, and that constraint is what
  makes the read path affordable.
- **Dependent on a metadata service.** No DynamoDB, no Postgres, no etcd, no
  ZooKeeper. If a design needs one, the design is wrong for this project.
- **A system whose request rate scales with records, shards, partitions or
  indices.** That is the failure this exists to prevent.
- **A durability compromise.** An acked write is in a regional, multi-AZ bucket
  before the ack. By default there is no local-disk write-ahead log and no single-AZ
  storage class on the primary path.

## Deployment constraint

**Self-managed OpenSearch only** (Kubernetes or VMs), because the consumer is a
**custom plugin** installed into the node process.

⚠️ AWS OpenSearch Service and equivalent managed offerings permit only an
allow-list of plugins and are therefore **out of scope**. Supporting one would
mean replacing the plugin with an external process writing via the `_bulk` API,
which forfeits, in order of severity:

1. **Offsets committed atomically with the documents** ([ADR-0005](decisions/0005-no-consumer-offset-store.md)) — an external writer would need its own offset store, the weaker design that ADR rejects.
2. **Zero idle cost** — the blocking `readNext` is an SPI property; an external poller has nothing to block on.
3. **Native backpressure** — `PartitionedBlockingQueueContainer` is inside the engine.

That is a different product, not a configuration. If it is ever wanted, it needs
its own ADR.

## What we trade away

Latency, deliberately and adjustably. Seconds are acceptable; the flush interval
and `maxStreamLatency` are the dials, and they are the operator's to turn.
