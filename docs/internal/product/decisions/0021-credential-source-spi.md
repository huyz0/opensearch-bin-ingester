# 0021. The credential identifies the trust domain; credentials come from a swappable SPI

Status: accepted
Date: 2026-08-30
Requirements: FR-20, and the security model in [ADR-0010](0010-multi-tenancy-and-security-model.md)

## Context

Two OpenSearch clusters, and **producers hold per-cluster credentials** — a
password or client credentials scoped to one cluster. Credentials must be
changeable at runtime, initially from a file, later from a sidecar.

## Decision

### 1. Authentication and trust-domain resolution are the same operation

A producer presents a credential; the gateway resolves it to a **trust domain**.
There is no separate "which cluster is this for?" field to get wrong, and a
producer cannot write into a domain its credential does not name.

```
credential ──▶ Principal { trustDomain, subject, allowedIndices }
                    │
                    ├── which segment stream and bucket prefix
                    ├── which object-store identity the writer uses
                    └── which ordinal registry resolves index names
```

Per trust domain, therefore: its own bucket prefix (`<prefix>/<clusterId>/…`,
already in the key grammar), its own object-store identity, its own ordinal
registry, and its own segment stream — segments are never bundled across domains.

**Cost of two domains: ~$31/month of PUTs at a 5 s interval**, versus $16 for one.
Linear, and modest at this scale.

### 2. Credentials come from a `CredentialSource` SPI

```java
public interface CredentialSource extends Closeable {
    /** Resolve a presented credential to a principal, or empty if unknown. */
    Optional<Principal> authenticate(Credential presented);
}
```

Implementations, in delivery order:

| Impl | When |
|---|---|
| `StaticCredentialSource` | M1 — a single domain, no auth. **The seam exists so nothing is retrofitted later** |
| `FileCredentialSource` | M2 — a file, hot-reloaded |
| `SidecarCredentialSource` | later — whatever the sidecar exposes |

The SPI covers both shapes the producer may present: a bearer/basic secret, and a
client certificate. `Credential` is a sealed type over those.

### 3. ⚠️ Hot reload must poll, not watch

A Kubernetes ConfigMap or Secret is mounted as a symlink tree — `..data` points at
a timestamped directory, and an update **swaps the symlink** rather than rewriting
the file. `WatchService` registered on the file path frequently misses that swap,
which is how a credential rotation silently fails to take effect until the next
restart.

**Poll the content hash on a short interval** (default 10 s), or watch the parent
directory for the `..data` replacement. Do not watch the file.

### 4. Handling rules

- **Constant-time comparison** for secrets. A timing-variable `equals` on a token
  is a real oracle.
- Secrets in the file are **hashed at rest** (argon2id or bcrypt), never plaintext.
- ⚠️ **Never log, trace, or place in an error message** the credential, the
  principal's secret, or a signed URL ([security.md](../../standards/security.md)).
  An authentication failure logs the *subject* and the reason, never the material.
- A reload that fails to parse **keeps the previous credential set** and alarms.
  Rotating into a broken file must not take ingestion down.
- Unknown credential ⇒ `401`. Known credential writing to an index outside
  `allowedIndices` ⇒ `403`. Neither is buffered, per ADR-0006's reject-never-fold.

## Alternatives considered

- **A trust-domain field in the request, authenticated separately.** Rejected: two
  things to keep in step, and a producer could name a domain its credential does
  not cover — an authorization check we would then have to remember to write.
- **Hard-coded credentials or environment variables.** Rejected: not rotatable
  without a restart, which is the requirement.
- **Reading the sidecar directly, no SPI.** Rejected: the file implementation is
  needed first, and a seam added later is a retrofit through the auth path.
- **`WatchService` on the credential file.** Rejected on the symlink behaviour
  above — it appears to work in a local test and fails in the cluster.

## Consequences

- M1 carries `StaticCredentialSource` and the `Principal` seam; **no auth logic**,
  but the trust domain flows through the write path from day one.
- The gateway holds **N object-store identities**, one per domain. Confusing them
  would cross a trust boundary, so the identity is resolved from the `Principal`
  and never from configuration at the call site.
- The plugin authenticates to the gateway through the same SPI, with a principal
  bound to its cluster — so a node can subscribe only within its own domain.
- ⚠️ Two domains means two of everything stateful: registry, prefix, identity,
  segment stream. **A test that runs with one domain will not catch a leak across
  two** — the conformance and end-to-end suites need a two-domain fixture, and it
  should exist before the second cluster does.
