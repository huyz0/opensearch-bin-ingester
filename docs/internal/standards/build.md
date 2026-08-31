# Build and test environment

**Family:** Delivery
**Read when:** Adding a test that needs a container, changing a memory setting, wondering why the default `./gradlew test` does not start Docker, or when WSL2 kills a session.

## The constraint

Development is **WSL2, 32 GB, several concurrent sessions** — so **8 GB is the
per-session budget**. ⚠️ WSL2 does not kill the offending process; it kills
whatever it likes. **Every memory limit must be set where the runtime enforces it,
so a runaway dies as a JVM `OutOfMemoryError` or a Docker OOM-kill, never as a
lost session.**

## Budget

| Component | Cap | Set where |
|---|---|---|
| Gradle daemon | 1 GiB | `org.gradle.jvmargs=-Xmx1g` in `gradle.properties` |
| Java compile daemon | 512 MiB | `options.forkOptions.memoryMaximumSize` in the conventions plugin |
| Test JVM | 512 MiB | `test { maxHeapSize = "512m" }` |
| MinIO container | 256 MiB | `--memory=256m` |
| OpenSearch container | 1 GiB | `--memory=1g`, `OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m` |
| Gateway under test | 512 MiB | `-Xmx512m` |
| Headroom / page cache | 1 GiB | — |
| **Total** | **5,888 MiB (5.75 GiB)** | of an 8 GiB budget |

⚠️ The compile-daemon and test-JVM rows are **per worker**, and
`org.gradle.workers.max=2`, so each counts twice — which is what
`scripts/test_budget.py` sums. A table counting one of each understates its own
gate by ~1 GiB, and a real breach would look compliant.

⚠️ **Not `org.gradle.java.compile-daemon.jvmargs`.** That looks like a Gradle
property and is not one — the string appears nowhere in the 9.7.0 distribution.
It was set, counted toward the budget, and enforcing nothing, which is the exact
failure this section exists to prevent. Set a limit where the runtime reads it,
and prove it by probing the realised task.

**Ceiling: 6 GiB**, which is what `scripts/check-test-budget.sh` sums the
configured limits against — the table above is the intended allocation, the
ceiling is the limit. The remainder of the 8 GiB is page cache and headroom.

Also: `--max-workers=2`, and a timeout on every test task so a hung test releases
its memory. → `scripts/check-test-budget.sh` asserts the configured limits sum
below the ceiling.

## Tiers map to Gradle tasks; containers are opt-in

| Task | Tiers | Containers | Memory | When |
|---|---|---|---|---|
| `./gradlew test` | T0–T2 | **none** | ~2 GiB | every commit, the default |
| `./gradlew integrationTest` | T3 | MinIO | ~2.8 GiB | on demand + CI |
| `./gradlew clusterTest` | T4 | OpenSearch (+MinIO) | ~4.9 GiB | on demand + CI |

1. **The default task starts no container.** A developer, or an agent running
   `/milestone`, gets a fast light loop; the heavy tiers are explicit.
2. **Never run `clusterTest` and `integrationTest` concurrently** locally. CI may.
3. **Every container declares `--memory`.** A container without one can take the
   session down with it.

## Scale-test cheap, integrate expensive

⚠️ **A test that needs scale and a test that needs realism are different tests.**
Do not buy both at once.

The zero-idle-cost criterion is the example: 1,600 *real* OpenSearch shards need
6+ GiB and minutes to start — infeasible here. **1,600 consumers against a fake
transport need ~200 MiB and seconds**, because they are parked virtual threads.

So: **scale at T1, realism at T4 with a small shard count.** The most valuable
test in the project then runs on every commit instead of only in CI.

## Where harness state lives

`.harness/` — review verdicts, TDD red records, suite timings. Gitignored, and
**deliberately not under `build/`**: that directory belongs to Gradle, whose
`clean` task deletes it. It once took the hash-bound review verdicts and red
records with it, so `./gradlew clean` silently turned a reviewed commit back into
an unreviewed one, and a recorded red run into no evidence at all.

⚠️ **State that outlives a build does not live in the build directory**, however
convenient the path is.

## Gate scope: fast by default, complete in CI

Every per-file gate has two modes, chosen by `GATE_SCOPE`:

| Mode | Examines | Used by |
|---|---|---|
| `delta` (**default**) | only files this change touches | the pre-commit hook |
| `full` | the whole tracked tree | CI, and any manual run |

⚠️ **Every gate prints which mode it ran in.** A `delta` pass is a weaker
statement than a `full` pass and must not read like one — non-negotiable 4.

Two rules keep `delta` sound:

1. **A gate escalates itself when the delta is not enough.** `check-links` goes
   full-tree whenever the change deletes or renames a file, because a link in a
   file this change never touched can break when its target moves. A gate that
   cannot be made sound in `delta` mode does not offer it.
2. **CI always runs `full`.** `delta` never re-examines a file that stopped
   being valid for a reason outside its own diff, so it is a speed optimisation,
   never the authority.

⚠️ **The saving is no longer nil.** A docs-only or script-only commit makes **no
Gradle call at all** — the delta path selects no module — while a shared-build
change escalates to all eight and costs seconds. Before `check-module` the whole
suite was ~525 ms either way.

⚠️ **No point value is given deliberately.** Two machines measured 11 s and 15 s
for the same warm case; a single number would be false precision. The case that
matters for the 90 s budget is the **cold** daemon after a WSL2 session kill, and
nothing has measured it — so treat the budget as unverified for that case rather
than as headroom.

⚠️ **The saving is proportional, with one cliff.** A change under `format/`
builds `format` alone; a change under two module directories builds two. The
cliff is the shared build — `buildSrc/`, `gradle/`, `settings.gradle.kts`,
`build.gradle.kts`, `gradle.properties` — which escalates to all eight, because
a shared build file can put a dependency on a module's classpath without naming
that module. `ModuleSelectionTest` is the authority on this mapping.

⚠️ **Delta scope matters more for the gates that are coming than for this one.**
Compile, JaCoCo and PIT scale with the code they examine, and `check-mutants.sh`
is diff-scoped by design: a whole-tree mutation score is dominated by code nobody
touched and moves too slowly to gate a commit.

## Execution layers — what runs when, and for how long

| Layer | Contents | Budget | Trigger |
|---|---|---|---|
| **L0** pre-commit, local | T0–T2 unit + the text gates | **≤ 90 s** | every commit, **blocking** |
| **L1** CI fast | L0 + cost assertions + **gate benchmarks** | **≤ 5 min** | every push/PR, **blocking** |
| **L2** integration | T3, MinIO | ≤ 10 min | **selective** — see below |
| **L3** e2e cluster | T4, OpenSearch | ≤ 15 min *total* with L2 | selective, same triggers |
| **L4** full benchmarks | JMH, proper fork counts | unbounded | **manual only** |
| **L5** profiling | async-profiler / JFR over e2e | unbounded | **manual only** |

⚠️ **A task that exceeds its budget fails.** → `scripts/check-suite-time.sh`. A
slow suite stops being run, and a suite that is not run is not a gate.

### Selective triggering for L2/L3

Not every change earns fifteen minutes. In order of precedence:

1. **Path filter** — changes under `binstore-backends/**` trigger L2; under
   `plugin/**` or `client/**` trigger L3; under `format/**` trigger both,
   because a format change reaches every reader.
2. **Label** — `run-integration` / `run-e2e` on a PR forces them.
3. **Nightly** — everything, on a schedule, so a path filter that was wrong is
   caught within a day rather than at release.
4. **Always before a milestone is declared complete** — `check-milestone-verified.sh`
   evidence lines for T3/T4 criteria cannot say `NOT-RUN` at that point.

⚠️ **The path filter is a guess about coupling and will eventually be wrong.**
The nightly run is what makes it safe to be wrong; do not drop it to save minutes.

## CI

GitHub Actions runners have more headroom than a local session, so CI runs
**every** tier including `clusterTest`, and `pre-commit run --all-files` against
the same `.pre-commit-config.yaml` the local hook uses — one definition, never
reimplemented.

⚠️ CI must **also** run under the same caps. A test that only passes with 16 GB is
a test that will fail locally, and the person it fails for will not be the person
who wrote it.

## Licensing

**Apache-2.0, public repository.** Same licence as OpenSearch, Helidon and AutoMQ,
so there is no friction taking dependencies from that ecosystem — and none if the
plugin-extensible `IngestionMessageMapper`
([ADR-0020](../product/decisions/0020-record-envelope-and-mapper.md)) is ever
offered upstream.

1. **Every source file carries `SPDX-License-Identifier: Apache-2.0`** in its first
   eight lines. → `scripts/check-license-headers.sh`. Free from the first commit;
   tedious to retrofit.
2. **No GPL, AGPL, SSPL, EUPL, CDDL-1.0 or BUSL dependencies**, and **every
   dependency jar carries a pinned SHA-1 and a committed licence** in `licenses/`.
   The mechanism is OpenSearch's `DependencyLicensesTask`, deliberately:

   | File | Meaning |
   |---|---|
   | `licenses/<artifact>-<version>.jar.sha1` | the exact artifact reviewed; a mismatch means it changed underneath us |
   | `licenses/<prefix>-LICENSE.txt` | the licence text, for attribution |
   | `licenses/SPDX.txt` | `<prefix> <SPDX-ID>` — **the machine-checkable claim** |

   → `./gradlew dependencyLicenses`, wired into `check` (and therefore `build`),
   is authoritative. ⚠️ It is deliberately **not** cacheable, diverging from
   OpenSearch's version: Gradle treats its module cache as immutable and does not
   re-hash it, so a jar whose bytes changed under an unchanged version left the
   task `UP-TO-DATE` with the mismatch unreported. Measured, not theorised. The
   cost of always running it is nil — 567 ms up-to-date against 543 ms forced,
   because the time is Gradle startup, not hashing ten small jars. `scripts/check-dependency-licenses.sh` is the fast pre-commit
   half and says so. `./gradlew updateShas` writes missing pins — and **only
   missing ones**: it will not overwrite a pin that disagrees, because that case
   is an artifact changing under a version already reviewed.

   ⚠️ **The deny-list matches the SPDX identifier, never the licence prose.**
   EPL-2.0's text names "GNU General Public License" in its Secondary License
   clause, so a prose scan rejects JUnit; and a substring match on `GPL-2.0` also
   rejects LGPL-2.0, a different licence. Identifiers are enumerated exactly.

   ⚠️ A copyleft dependency found late is expensive to remove, so this runs from
   M0 rather than before release. Adding a dependency is a **diff** — a pin, a
   licence text and an SPDX line — not a silent resolve.
3. **An exception needs a line in `baselines/licenses.txt`** naming the dependency,
   the licence and who reviewed it. A growing file is a signal.
4. `LICENSE` is the canonical Apache text, fetched from apache.org, not retyped.
   `NOTICE` names the project.
5. ⚠️ **The research corpus is public too.** It carries cost figures, vendor
   comparisons and cloud pricing. Nothing in it should be commercially sensitive —
   check before adding a customer name, an internal hostname or a real bucket.

## Version skew between plugin and ingester

The plugin lives in OpenSearch nodes that upgrade on their own schedule; the
ingester is a separate deployment. **They will diverge.**

6. **The subscription protocol negotiates a version on connect**, and both sides
   support **N-1**.
7. ⚠️ **The ingester speaks the older dialect**, never the reverse. A plugin upgrade
   requires a node restart and an OpenSearch maintenance window; an ingester upgrade
   is a rolling deploy. **The side that is cheaper to move is the side that adapts.**
8. Segment and commit-log formats follow the separate rule that **readers ship one
   release before writers** ([50-open-questions Q11](../../research/50-open-questions.md)).

## WSL2 notes

- Cap WSL2 itself in `%UserProfile%\.wslconfig` (`memory=`, `swap=`) so it cannot
  consume the whole host.
- Prefer Docker running **inside** WSL2 over Docker Desktop — one less VM.
- Testcontainers needs a reachable Docker socket; check it before assuming a test
  hang is our bug.
