# 0041. A signed URL is a capability and a type, not a string

Status: accepted — ⚠️ **amended 2026-09-11 by M5.14** on ownership only: the Consequences below assign the grant's delivery to the subscription protocol, and the range scoping to "M5.14's coordinates". M5.14 shipped the event SHAPE ([ADR-0042](0042-the-subscription-event-carries-a-session-and-an-epoch.md)) and deliberately carried NEITHER — `via=DIRECT` says the consumer fetches for itself, and no field yet holds the URL or a byte range. ⚠️ **M5.44 owns both**, and this note exists because the first draft of ADR-0042 deferred them to no row at all, which would have left this record pointing at a task that had already declined the work. The DECISIONS here are untouched.
Date: 2026-09-10
Requirements: FR-6, NFR-5
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md §2 and §4
  bound `direct` — its fan-out rule and its cache-destruction trap are M5.11's.
  ⚠️ §6 of that same document is OVERTURNED by this record and carries a banner
  saying so: it claims the ingester "mints it locally… no API call", which is
  false for a keyless GCS signer, and its Local FS row is aspirational.
  docs/research/30-design-space/07-pluggable-store-abstraction.md is the store-SPI
  contract document and carries a banner for this change. ADR-0023 decides that the consumer holds no `BinStore`; this record does
  not reopen that — a signed URL is plain HTTP, which is why `direct` can exist
  at all without giving the OpenSearch JVM a store client.

## Context

FR-6 serves record bytes in three modes and `direct` is the one where the
consumer reads the object store itself, over a short-lived signed URL. Two
things about it are decided here because both are easy to get wrong quietly.

**No shipping backend can presign.** `MemoryBinStore` has no URL space at all
and `LocalFsBinStore` has no HTTP surface. So `direct` is unreachable today, and
the question is what a deployment that asks for it should experience.

**A signed URL is a credential with a timer.** security.md rule 4 puts it in the
same sentence as credentials and document payloads: never logged, traced or put
in an error message. It is also the one value in this system that is *useful to
an attacker on its own* — it needs no identity, and it works from anywhere until
it expires.

## Decision

**`presign` is a capability advertised in `Capabilities`, and its result is a
type whose `toString` redacts.**

- **`Capabilities.presignedUrls`, with `requirePresignedUrls()`** — the shape
  `requireConditionalWrites` already establishes (ADR-0008). A deployment that
  wants `direct` refuses to start on a backend that cannot do it, rather than
  discovering it at first use. ⚠️ First use is the worst possible moment: the
  ingester chooses `direct` when it is under pressure or at fan-out 1, so the
  failure would arrive exactly when the alternative paths are least able to
  absorb it.
- **`BinStore.presign(key, ttl)` defaults to refusing**, so a backend that has
  not implemented it cannot return something unusable and call it a URL.
- **One key, and a caller-supplied TTL** — security.md rule 3. There is no
  prefix or bucket form. A grant covering more than the object being served is
  wider than the fetch needs, and its holder is inside the OpenSearch JVM.
- **`SignedUrl` is a record whose `toString` redacts** and whose expiry is
  shown.

## Alternatives considered

**(a) Return a `String`.** Rejected, and this is the substantive half of the
record. The rule "never log a signed URL" is then enforced by every future
caller remembering it — non-negotiable 9 calls that the weakest rung there is,
and it is invisible outside the prompt that said it. A redacting type moves the
guarantee to rung 1 for the three paths a secret actually escapes by: string
concatenation into a log line, an exception message built from the value, and
the generated `toString` of any record, list or map that HOLDS one. All three
route through `toString`, and all three are tested.

**(b) Throw from `presign` and skip the capability.** Rejected: it is the same
information delivered at the moment it cannot be acted on. The capability is
what lets a misconfigured deployment fail at startup.

**(c) Let `presign` return `Optional.empty()` when unsupported.** Rejected: it
invites a caller to treat "this store cannot do it, ever" as a per-call miss and
retry, and it puts the check on the hot path where the capability belongs at
startup.

**(d) Give the consumer a `BinStore` instead.** That is ADR-0023's rejected
design and stays rejected. A signed URL is plain HTTP; a store client is a
credential, an SDK and a cloud dependency in the OpenSearch JVM.

## Consequences

**`direct` is unreachable until a backend implements `presign`.** Both shipping
backends advertise `presignedUrls=false`, so a deployment enabling `direct`
refuses to start. That is the intended behaviour and not a gap: FR-6's other two
modes are unaffected, and M5's SPEC already says the ingester chooses the mode.

**The conformance suite pins BOTH answers.** `PresignConformance` sits in the
chain every backend already runs, and the case it exists for is the pair
DISAGREEING — a backend advertising the capability and throwing anyway would
pass a startup check and fail at first use, which is precisely what the
capability is supposed to prevent.

⚠️ **A BACKEND MUST SIGN WITH NO REQUEST PER SIGNATURE, or advertise
`presignedUrls=false`.** This is the cost half of the decision and it is stated
here because two banners and `BinStore.presign`'s javadoc cite this record for
it. S3 SigV4 signs locally from static credentials; an Azure user-delegation SAS
signs locally too, from a delegation key fetched once and valid up to seven days
— one request amortised over every URL it signs, which is why the line is drawn
PER SIGNATURE rather than at zero. ⚠️ The rejected case is a signature needing a
round trip for each URL — GCS V4 signing from a keyless Workload Identity,
through `iam.signBlob` — because `direct` exists to take a fetch OFF the
ingester, and a per-URL round trip puts a second request back on the path for
every one it removes. Such a backend answers `presignedUrls=false` and the
deployment falls back to `proxy`. ⚠️ **This half is not mechanically checked and
cannot be**: a meter wrapping a backend cannot see the backend's own internal
traffic, which `PresignConformance` records and review MEASURED.

**`toString` is a load-bearing method.** Anyone adding a field to `SignedUrl`
must not add it to a generated `toString`, and anyone tempted to "improve"
debuggability by printing the URL is removing the guarantee. The test names the
three escape paths so the reason survives the next edit.

⚠️ **THE RANGE IS NOT SCOPED, and security.md rule 3 asks for it.** That rule
wants a grant "read-only, scoped to one key and where possible one range";
`presign(key, ttl)` implements the first two and not the third, so the shipped
grant covers a whole segment. That is deliberate rather than overlooked: a
segment is the unit a consumer fetches and the unit `SegmentKey` addresses, and
a range would have to come from the subscription protocol's coordinates, which
are M5.14's. ⚠️ Recording it because "where possible" is the kind of clause that
silently becomes "never" if nobody writes down that it was considered.

⚠️ **What this does NOT decide: how the URL reaches the consumer.** That is the
subscription protocol's, and it is M5.14 — where the wire-format obligations
land, because that is the first point at which a signed URL crosses a process
boundary. Nothing in this record changes any serialized form.
