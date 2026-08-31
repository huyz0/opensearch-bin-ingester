# Security

**Family:** Quality
**Read when:** Touching credentials, signed URLs, tenant isolation, the subscription protocol, or anything parsing untrusted input.

1. **The bundling universe is a trust domain.** Never co-bundle segments across
   OpenSearch clusters or tenants — a segment is readable as a unit, so bundling
   across trust domains makes any grant a leak.
   ([`fetch-modes §6`](../../research/30-design-space/10-client-library-and-fetch-modes.md))
2. **Object-store credentials never appear in index settings.** Index settings
   are cluster state. Use the OpenSearch keystore or ambient identity.
3. **Prefer signed URLs over credentials in the OpenSearch JVM.** Short TTL,
   read-only, scoped to one key and where possible one range.
4. **Never log, trace or put in an error message**: credentials, signed URLs, or
   document payloads.
5. **Anything sized by client input has a bound.** Frame lengths, batch counts,
   key lengths, queue depths.
6. **Reject, do not fold, on a protocol mismatch.** A producer claiming a
   partition count the index does not have is an error, not a modulo.
7. **Untrusted bytes are never parsed on the ingester hot path.** Routing metadata
   arrives out of band in the framing.
8. ⚠️ **The security model is an open question** — see
   [`50-open-questions.md`](../../research/50-open-questions.md) Q10. These rules
   are the part already decided, not a complete model.
