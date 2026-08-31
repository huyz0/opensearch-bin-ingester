---
name: tdd
description: Implement a task test-first in Java. Use when writing any code. Covers the red-green cycle, what to assert, the test tiers, and the rules that keep the resulting test worth having.
---

# Test-driven implementation

## The cycle

1. **Write the failing test first**, from the milestone's test plan.
2. **Run it and record the red run**, with **fully-qualified** ids:
   ```
   scripts/tdd-red.sh 'binjava.format.SegmentTest#directoryBinarySearchFindsRun' …
   ```
   It runs them, reads the JUnit XML the run produced, and records only the ids
   that actually appear there **as failed**. ⚠️ A compile error is not a red run:
   it exits non-zero having run nothing, and the script says so rather than
   recording it. ⚠️ If they pass before the production code exists, they are not
   testing the change — fix the test.
3. **Implement the smallest thing that passes.**
4. **Run it and watch it pass.**
5. Refactor with the test green.

`scripts/check-tdd.sh` refuses a commit whose new tests have no red record. It
finds them by diffing the whole set of test methods before and after, so
`@ParameterizedTest`, a `@Test` sharing a line with its signature, a `@Nested`
class and a class not named `*Test` are all covered.

Each record is bound to the **sha256 of the test file it was observed in**, so
editing the test after watching it fail invalidates the record: what was seen
failing has to be what is committed.

⚠️ **The gate raises the cost of skipping; it does not prove virtue.**
`.harness/tdd/red.json` is an unsigned file in a gitignored directory — anyone
willing to write one line of JSON can forge a record, and no script can stop
them. It exists so that skipping is a decision rather than a drift, exactly like
non-negotiable 3.

⚠️ **Production code changes to satisfy the test. Never the reverse.** If a test
turns out to be wrong, fix it as its own change with its own reasoning — do not
loosen an assertion in the same commit that changes production code.
`scripts/check-test-integrity.sh` flags exactly that.

## What to assert

- **Behaviour, never implementation.** A test that breaks on refactor and passes
  when behaviour breaks is worse than none.
- **The acceptance criterion from the task**, in terms a reader recognises.
- Name the test for the behaviour: `idleShardsIssueNoObjectStoreRequests()`, not
  `testConsumer2()`.

## Tiers

| Tier | What | Backing store |
|---|---|---|
| **T0** | pure logic, no I/O — formats, filters, offsets, coalescing | none |
| **T1** | component with fakes | `MemoryBinStore` |
| **T2** | end-to-end, single JVM | `LocalFsBinStore` |
| **T3** | real object-store semantics | MinIO / LocalStack via Testcontainers |
| **T4** | inside OpenSearch | `internalClusterTest` |

Default to **T0**. If a test seems to need a socket, a clock, or an object
store, ⚠️ **that is a signal the logic is in the wrong layer**, not a reason to
move up a tier. Inject the seam instead — see
[`contracts` in code-structure.md](../../../docs/internal/standards/code-structure.md).

⚠️ **T3 is not a substitute for T0.** Conditional-write semantics differ per
provider, so the [store conformance suite](../../../docs/internal/standards/testing.md)
must run against **every** backend — that is the highest-leverage test in the
project.

## Rules broken most often

- **No `Thread.sleep`.** Inject a clock; use `Awaitility` only where a real
  external system is involved.
- **No fixed ports.** Bind `0`.
- **Scratch under `build/tmp`**, never the system temp directory.
- **Prefer a fake over a mock.** `MemoryBinStore` over `Mockito.mock(BinStore)`.
- **Bounded memory is a test target**, not a hope — see the soak test in
  [`docs/research/40-implementation/02-streaming-io-and-memory.md`](../../../docs/research/40-implementation/02-streaming-io-and-memory.md) §7.

## Before you call it done

- Every acceptance criterion **observed** to be met, not expected to be.
- `scripts/check-module.sh <module>` green (⚠️ see AGENTS.md — does not exist until
  the build does; until then, say which commands you ran).
- **Coverage ≥95% line / ≥90% branch** for the module, and **mutation score ≥80%**
  on the changed code. ⚠️ Coverage is a floor, not a measure: a line can execute
  without being constrained. Mutation score is the number that says whether the
  tests constrain anything.
- **Both reviewers run** — `reviewer` on the production diff and `test-reviewer` on
  the tests. See [`review`](../review/SKILL.md).
- If the change touches a hot path, run [`bench`](../bench/SKILL.md).
- If the change touches the object-store call pattern, run
  [`cost-budget`](../cost-budget/SKILL.md). **A change that silently adds a
  request per record passes every other gate.**
