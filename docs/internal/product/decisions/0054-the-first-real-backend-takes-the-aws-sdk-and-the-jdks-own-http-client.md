# 0054. The first real backend takes the AWS SDK, and the JDK's own HTTP client

Status: accepted
Date: 2026-09-18
Requirements: NFR-8, NFR-2, NFR-10
Research: docs/research/40-implementation/01-java-runtime-helidon-vthreads.md, docs/research/30-design-space/07-pluggable-store-abstraction.md

## Context

ADR-0052 books the first real object-store backend as one of M8's first tasks,
because RPO 0 measured against a `HashMap` in the killed process's own heap is a
measurement of the fixture. That leaves a question ADR-0052 does not answer: how
this project talks S3.

Two things constrain the answer more than performance does.

**This build pins every dependency jar by SHA-1 and commits a licence text for
it.** Accepting a dependency is a diff someone reads, not a line in a generated
report — so the size of a dependency's transitive graph is a cost paid in review
attention, once per jar, and again on every upgrade.

**The store is the only durable dependency, and `binstore-backends` is the one
module allowed to touch I/O** (AGENTS.md non-negotiable 7). Whatever is chosen
is reachable from nowhere else, so the blast radius of getting it wrong is one
module — but the SPI's contract is not negotiable: blocking calls, `IOException`
on every method, a conditional write that answers "you lost" without throwing,
and one call being one request.

## Decision

**The AWS SDK for Java v2, synchronous client, with
`url-connection-client` as its HTTP client and both of its own HTTP clients
excluded from the build.**

- **The SYNCHRONOUS client, not the async one.** Research 40-01 §1 settles this
  for the whole project: JEP 491 removed virtual-thread pinning on
  `synchronized`, so a blocking call IS the concurrent API here and no library
  has to be audited for it. The async client would also drag in a reactive
  runtime, and every call site above the SPI is written blocking.
- **`url-connection-client`**, which is the JDK's own `HttpURLConnection`.
  Netty (`netty-nio-client`) and Apache HttpClient 5 (`apache5-client`) are
  excluded in `binjava.AwsSdkHttp`, from one list that drives both the module's
  dependency declaration and the root project's licence gate. The measurement:
  with them, `updateShas` pinned **45** jars; without, **30**.
- **Exclusions live in one Kotlin object, not in two build files.** The module's
  exclusion is what keeps a jar off a runtime classpath; the root's
  `licenseCheck` configuration is built from the version catalogue and cannot
  see a module's, so both are needed and they must agree. Written twice they
  drift, and the drift is silent in the direction that matters — the gate stops
  demanding a licence for a jar that is still shipped.

## Alternatives considered

- **Hand-rolled SigV4 over Helidon's `WebClient`.** This is the closest call,
  and its case is real: zero new jars, and this project already depends on
  Helidon for the front door. Rejected on the surface area rather than on
  taste — SigV4 signing (including the payload-hash and chunked variants),
  XML parsing for `ListObjectsV2` and `DeleteObjects`, error-code mapping,
  retry classification, endpoint resolution and credential providers (instance
  role, web identity, profile, refresh) are each a thing to get wrong, and the
  one that fails silently is signing: a wrong canonical request is a 403 in
  staging and a 403 at 3am against a different endpoint. ⚠️ The decisive
  number is not lines of code, it is **which failures are invisible**: an SDK
  that mis-signs is a bug report from thousands of users, and a hand-rolled
  signer that mis-signs is a bug report from this repository's own tests, which
  run against MinIO — an endpoint that is deliberately more permissive than S3.
- **MinIO's own Java client.** Rejected: it is one implementation's client, and
  what this project needs is the protocol every implementation speaks. It would
  also make the FIXTURE's vendor the PRODUCTION dependency, which is exactly
  backwards from testing.md rule 19a.
- **The SDK's `S3TransferManager` or CRT client.** Rejected: both add a native
  or reactive layer for parallel transfer this project does not do — segments
  are bounded at 8 MiB by `IngestConfig`, so a multipart upload here is a
  handful of parts, not a gigabyte fan-out.
- **Keeping Netty and paying for the jars.** Rejected: fifteen extra jars, each
  needing a pinned sha and a committed licence, for an HTTP stack no call site
  reaches.

## Consequences

- **A container is in the test path** (build.md's memory cap, Docker off by
  default). The T3 suite is `./gradlew integrationTest`, and it SKIPS rather
  than fails where no Docker daemon answers — a developer without one gets a
  skip that says so, not a red suite they cannot act on.
- **MinIO is the fixture and is not S3** (testing.md rule 19a). Where the
  protocol is ambiguous the two differ, so the cases assert the shapes this
  project depends on — 412 for a lost conditional write, 404 for one with
  nothing to match — and M8's `VERIFIED.md` must say which criteria rest on
  MinIO alone. ⚠️ Conditional writes are the sharpest edge: ADR-0011's CAS is
  `If-None-Match`, both support it, and the ERROR SHAPES differ.
- **`presignedUrls` stays false until M8.18.** The SDK can presign locally from
  the credential — the `BinStore.presign` contract's "no request per signature"
  is satisfied — but M5.42's obligation that a signing failure's own message
  carries neither a URL nor a credential is a behaviour, not a signature, and it
  is M8.18 that buys it. Advertising the capability first is how `direct` gets
  enabled against a backend that cannot sign.
- **An upgrade is a 30-line diff of sha1 files.** That is the cost of the pinning
  rule, paid deliberately: the alternative is a supply-chain input nobody reads.
- ⚠️ **What this does NOT buy is AWS itself.** Every case here runs against
  MinIO. The first run against a real S3 endpoint is unscheduled work, and the
  honest statement is that S3-specific divergence — request-rate throttling,
  eventual consistency of a bucket listing after a delete, per-prefix rate
  limits — remains unmeasured by this project.
