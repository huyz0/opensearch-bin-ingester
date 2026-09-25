# 0071. Make assembled lease construction observable in tests

Status: accepted
Date: 2026-09-26
Requirements: FR-11, NFR-9

## Context

The Kubernetes pod UID is written into both sequencer and GC lease objects.
Unit tests of `LeaseManager` prove configured UIDs are serialized, but do not
prove the composition root supplies that value to each manager. A GC test that
only checks a lease-config helper also misses a composition regression that
bypasses the helper. The GC lease is acquired only when retention work is due,
so driving a full retention pass is an indirect and timing-sensitive way to
observe construction.

## Decision

Keep a package-private `LeaseManagerFactory` seam on `Assembly`'s test path.
Production uses the method reference to the real `LeaseManager` constructor;
the assembly test captures the GC manager created by the real composition path,
acquires its lease, and decodes the stored object. Move retention-loop
construction into `RetentionAssembly`, keeping the composition root below its
700-line file limit.

The seam changes no runtime behavior and adds no store operation. Tests exercise
the same manager and `BinStore` as production.

## Alternatives considered

- **Inspect a config helper only:** rejected because replacing the composition
  call with a compatibility constructor would leave the helper test green while
  production GC leases omitted the UID.
- **Use reflection into `Assembly`/`RetentionLoop`:** rejected because tests
  would depend on private object layout, and that layout does not prove that the
  manager actually writes to the assembled store.
- **Wait for a naturally due GC pass:** rejected because a fresh segment usually
  does not trigger lease acquisition; aging it far enough also exercises
  retention decisions unrelated to lease construction.

## Consequences

The test-only factory is an additional package-private seam, with the default
constructor path fixed in production. A deliberate test can now verify both
sequencer and GC lease bytes at the composition boundary without reflection or
waiting for a timer. Lease wire bytes and object-store request counts are
unchanged by this test seam.
