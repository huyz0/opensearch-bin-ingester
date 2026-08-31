# Packaging the plugin

**Status:** draft · **Confidence:** medium (build mechanics verified against 3.8.0 tree; the
security-policy story is in flux upstream and must be re-checked at implementation time) ·
**Last updated:** 2026-08-29

**Read this if:** you are setting up the plugin's build, dependencies, or permissions.
**One-line takeaway:** copy `plugins/ingestion-fs` for structure and `plugins/repository-s3` for
cloud-SDK packaging and permissions; keep the dependency surface tiny because plugin classloaders
and third-party audit make fat dependency trees painful.

---

## 1. Structure to copy

```
plugins/ingestion-fs/src/main/java/org/opensearch/plugin/ingestion/fs/
  FilePlugin.java            -> BinStorePlugin           (extends Plugin implements IngestionConsumerPlugin)
  FileConsumerFactory.java   -> BinStoreConsumerFactory
  FileSourceConfig.java      -> BinStoreSourceConfig      (parses params map)
  FileOffset.java            -> BinStoreOffset            (LongPoint-backed pointer)
  FileMessage.java           -> BinStoreMessage           (Message<byte[]>)
  FilePartitionConsumer.java -> BinStorePartitionConsumer
```

Six classes is the whole surface. Everything heavy (subscription, cache, coalescer) lives behind
`Plugin.createComponents()` as node-level singletons — see
[poller-semantics](02-poller-semantics-and-cost.md) §4.

`build.gradle` skeleton, from `plugins/ingestion-kafka/build.gradle`:

```gradle
opensearchplugin {
  description = 'Pull-based ingestion plugin that consumes from an object store'
  classname   = 'org.opensearch.plugin.binstore.BinStorePlugin'
}
dependencies { /* keep this list short */ }
thirdPartyAudit { ignoreMissingClasses(...) }   // expect to need this for any cloud SDK
```

## 2. Dependency strategy — the important decision

The AWS SDK v2 pulls in a large tree (Netty for the async client, Jackson, reactive-streams) and
every one of those needs `thirdPartyAudit` entries and permission grants. Options:

| Option | Pros | Cons |
|---|---|---|
| AWS SDK v2 sync client (`apache-client` or `url-connection-client`) | Official, S3-complete, blocking API fits virtual threads perfectly | Heavy-ish tree; still needs audit entries |
| AWS SDK v2 async (Netty/CRT) | Fast | Netty in the OpenSearch JVM, extra permissions, no benefit once we have virtual threads |
| Hand-rolled S3 REST over `java.net.http.HttpClient` | Zero deps, exact control of range GETs and conditional headers, trivially portable to GCS/Azure XML APIs | SigV4 signing to implement and test; must handle retries/redirects ourselves |
| Reuse OpenSearch's bundled `repository-s3` client | Already packaged and permissioned | Not exposed as an API to other plugins; coupling to internal classes |

**Leaning:** the **sync SDK v2 client with virtual threads** for v1 (correctness first), with the
hand-rolled path as a measured alternative — since the plugin only needs `GET` with `Range`, this
is a genuinely small surface and a good JMH comparison (see
[benchmarking-plan](../40-implementation/03-benchmarking-plan.md) §4). Decide with data, not taste.

> Note the asymmetry: the **service** needs the full write path (multipart, conditional PUT, LIST,
> DELETE); the **plugin** needs essentially only ranged GET plus one HTTP client for the tail
> channel. Do not force one client choice on both. Do share the *SPI* and the wire formats.

## 3. Credentials

Bucket credentials must **not** live in `index.ingestion_source.param.*` — index settings are part
of cluster state and are widely readable. Instead:

- Declare secure settings in `Plugin.getSettings()` (`SecureSetting.secureString`), read from the
  OpenSearch keystore, exactly as `repository-s3` does.
- Keep only non-secret locators in `param.*`: bucket/container, prefix, endpoint, region,
  service discovery URL, AZ hint.
- Prefer ambient credentials (IRSA / workload identity / managed identity) in production; the
  keystore path is the fallback.

## 4. Permissions / security policy

`plugins/repository-s3/src/main/plugin-metadata/plugin-security.policy` grants
`accessDeclaredMembers`, `getClassLoader`, `setContextClassLoader`, plus socket permissions.
Expect to need the same for any HTTP client, plus outbound connect permission to the object-store
endpoint and to the ingester.

✅ **Verified (2026-08-30): the declaration format is unchanged, the enforcement moved.**
OpenSearch 3.8.0 still reads `plugin-security.policy` (`PluginInfo.OPENSEARCH_PLUGIN_POLICY`,
`bootstrap/Security.java:496`), while enforcement is now a Java agent — `jvm.options` carries
`21-:-javaagent:agent/opensearch-agent.jar` and `libs/agent-sm/agent-policy` ships its own
`PolicyParser`. **Write the policy file as before.**

Thanks to [ADR-0004](../../internal/product/decisions/0004-the-service-serves-reads.md) the plugin
no longer reads the object store on any hot path, so its permission surface is just outbound
connections to the ingester endpoints — a much smaller policy than `repository-s3`'s.

## 5. Testing tiers

| Tier | Tool | What it proves |
|---|---|---|
| Unit | JUnit | Offset arithmetic, format encode/decode, Bloom/bitmap filter |
| Single node | `FileBasedIngestionSingleNodeTests` pattern (`plugins/ingestion-fs/src/test/`) | End-to-end ingest against a **local-FS** store — no containers, fast, this is the workhorse |
| Integration | `internalClusterTest` + Testcontainers (MinIO or LocalStack), as `ingestion-kafka` does with Kafka | Real S3 semantics incl. conditional writes |
| Cost | Counting store decorator | Asserts requests/MiB and **zero idle requests** ([cost-model](../00-problem/02-cost-model.md) §7) |

The local-FS store implementation is not just a convenience — it is what makes the single-node test
tier possible, which is why constraint C1 lists it explicitly.

## 6. Version compatibility

The SPI is annotated `@PublicApi(since = "3.6.0")`, and the two-step
`initialize` + `createShardConsumer(clientId, shardId)` factory methods are
`@Deprecated(forRemoval = true)`. Target **3.8.0+**, implement only the `IndexMetadata` overload,
and pin the tested OpenSearch version in CI.
