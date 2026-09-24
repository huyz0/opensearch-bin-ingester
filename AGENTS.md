# opensearch-bin-ingester

A lightweight ingestion service that bundles writes from many indices and
partitions into a small number of object-store objects, plus an OpenSearch
plugin that consumes them through the pull-based ingestion SPI. It replaces
Kafka in the OpenSearch ingest path at roughly **1/40th of the marginal cost**,
trading seconds of latency for it. Java 25, Helidon SE 4, virtual threads,
object storage as the only durable dependency.

This file is loaded into every session by every agent tool. It is deliberately
an index. Detail lives in the linked files.

## Start here

- [docs/internal/product/mission.md](docs/internal/product/mission.md) — what this is and what it must never become
- [docs/internal/product/requirements.md](docs/internal/product/requirements.md) — FR/NFR IDs that specs and tasks cite
- [docs/internal/product/architecture.md](docs/internal/product/architecture.md) — component map and the seams
- [docs/internal/product/roadmap.md](docs/internal/product/roadmap.md) — milestones in execution order, each with a completion condition
- [docs/internal/product/backlog.md](docs/internal/product/backlog.md) — the current task list, authoritative, current milestone only
- [docs/internal/product/decisions/](docs/internal/product/decisions/) — architecture decision records

## The research corpus

[docs/research/](docs/research/README.md) is 21 documents compiled **before any
code existed**: the cost model, prior art read from source (WarpStream, AutoMQ,
KIP-1150, SlateDB, Quickwit), the OpenSearch 3.8.0 SPI, and this project's own
design. **It is upstream of the product docs, not parallel to them.**

⚠️ **Do not read it wholesale and do not re-derive what it answers.** Start at
its README — it has a tier table and a task-based routing table — or use the
[`research`](.agents/skills/research/SKILL.md) skill.

Two things to know before trusting any single page: several documents carry ⚠️
**revision banners** where a later finding at real scale overturned an earlier
one, and **when an early conclusion and a revision banner disagree, the banner
wins**. And [50-open-questions.md](docs/research/50-open-questions.md) is the decision
log: **every question is answered**, ten of them as
[ADRs](docs/internal/product/decisions/). Six constants remain *deferred to
measurement* — that is a list of things to measure, not a list of things to
decide. ⚠️ Re-opening a settled decision is allowed, but it is an ADR, not a task.

## Vocabulary

Six roles, one name each. ⚠️ Binding, and enforced —
[glossary.md](docs/internal/standards/glossary.md).

```
producer ──bulk/202──▶ ingester  (K8s pods, 3 AZ, scalable; disks only when wal=true)
                        ├── writer ──▶ object store / WAL
                        └── reader ◀── object store / WAL / peer ingester node
                                ▲
                          consumer  (library)
                                │ bundled into
                          plugin  (in the OpenSearch node process)
```

Deprecated synonyms are listed in the glossary and **rejected at commit time**.
→ `./gradlew gates`

## Standards

Rules that are always true. Read the one that covers what you are touching.
Each names its gate, or is marked as having none — **the second kind matters
more, because it is where judgement is still required.**

<!-- index:standards:start -->
| Family | Standard | Read when |
|---|---|---|
| Process | [git.md](docs/internal/standards/git.md) | Before committing, when unsure whether a change is one commit or several, or before amending anything already pushed. |
| Process | [glossary.md](docs/internal/standards/glossary.md) | Writing any document, skill, ADR, commit message, log line, metric name, or identifier. These terms are binding. |
| Process | [review.md](docs/internal/standards/review.md) | Writing a review prompt, deciding whether a finding blocks a commit, or wondering why the reviewer was not given the author's reasoning. |
| Process | [sdd.md](docs/internal/standards/sdd.md) | Specifying work, decomposing a milestone, writing acceptance criteria, or when a spec turns out to be wrong. |
| Quality | [cost.md](docs/internal/standards/cost.md) | A change touches how objects are written, read, listed or discovered; before claiming a change is cost-neutral; or when a request rate appears in a design. |
| Quality | [observability.md](docs/internal/standards/observability.md) | Adding a metric, a log field, a span attribute, or a dashboard; or when you want per-index detail and are about to reach for a label. |
| Quality | [performance.md](docs/internal/standards/performance.md) | Before optimizing, when adding or changing a benchmark, when a change touches a hot path, or when profiling. |
| Quality | [security.md](docs/internal/standards/security.md) | Touching credentials, signed URLs, tenant isolation, the subscription protocol, or anything parsing untrusted input. |
| Quality | [testing.md](docs/internal/standards/testing.md) | Writing any test, choosing a tier, setting or reading a coverage gate, or when a test is slow, flaky, or passes without constraining anything. |
| Delivery | [build.md](docs/internal/standards/build.md) | Adding a test that needs a container, changing a memory setting, wondering why the default `./gradlew test` does not start Docker, or when WSL2 kills a session. |
| Code | [code-structure.md](docs/internal/standards/code-structure.md) | Adding a module, package or file; when a file nears 700 lines; or when deciding where a seam belongs. |
| Code | [java-style.md](docs/internal/standards/java-style.md) | Naming things, choosing a concurrency construct, writing buffer-handling code, or when a diff is hard to read for reasons code-structure.md does not cover. |
<!-- index:standards:end -->

⚠️ **A rule whose script is missing is a preference.** Most of these have no
script yet, because this repository has a harness before it has a build. That is
deliberate and temporary; see *Gates* below for what actually runs today.

## Skills

Procedures, in [.agents/skills/](.agents/skills/README.md), written to the Agent
Skills spec so they work in any tool that reads `SKILL.md`. A skill calls a
script in `scripts/`, never a tool-specific built-in.

<!-- index:skills:start -->
| Skill | Use when |
|---|---|
| [`adr`](.agents/skills/adr/SKILL.md) | When making a choice that is expensive to reverse, when changing a wire format or the store SPI, when a research conclusion is overturned, or when a future reader would otherwise ask "why on earth is it done this way" |
| [`bench`](.agents/skills/bench/SKILL.md) | When a change touches a hot path, when adding or changing a JMH benchmark, when a latency or throughput number is claimed, or when tempted to optimise anything |
| [`cost-budget`](.agents/skills/cost-budget/SKILL.md) | Whenever a change touches how objects are written, read, listed, or discovered — and before claiming any change is cost-neutral. This project exists to control these numbers |
| [`gate-design`](.agents/skills/gate-design/SKILL.md) | When adding a gate, a review step, a research step, or any rule an agent is expected to follow — and before writing an instruction that says "remember to" or "make sure you" |
| [`milestone-review`](.agents/skills/milestone-review/SKILL.md) | When a milestone reaches its completion condition or a checkpoint, and the commits need reading together rather than one at a time |
| [`milestone`](.agents/skills/milestone/SKILL.md) | When told to work through a milestone, or when a session should keep going until the milestone's completion condition is met |
| [`next-task`](.agents/skills/next-task/SKILL.md) | At the start of any working session, after finishing a task, or when unsure what to do. Prevents starting work that is blocked, unspecified, or already done |
| [`research`](.agents/skills/research/SKILL.md) | Before any web search or design argument about object storage, cost models, OpenSearch ingestion, WarpStream/AutoMQ, CAS coordination, or Java runtime choices — the answer is usually already here, with numbers |
| [`review`](.agents/skills/review/SKILL.md) | Before every commit. Defines what the reviewer is given, what it is deliberately denied, and what to look for that the deterministic gates cannot see |
| [`skill-forge`](.agents/skills/skill-forge/SKILL.md) | When a procedure has been explained more than twice, when a skill is not being invoked at the right moment, or when adding a command adapter |
| [`spec`](.agents/skills/spec/SKILL.md) | When starting a milestone, when a task lacks acceptance criteria, or when what to build is clearer than how it will be checked |
| [`tdd`](.agents/skills/tdd/SKILL.md) | When writing any code. Covers the red-green cycle, what to assert, the test tiers, and the rules that keep the resulting test worth having |
| [`wire-format-change`](.agents/skills/wire-format-change/SKILL.md) | Whenever bytes that outlive a process, or cross a process boundary, change shape |
<!-- index:skills:end -->

**Progressive disclosure.** This file is layer 0 and is deliberately an index.
Skill *descriptions* are layer 1 and cost a few hundred words. A skill's *body*
is layer 2. Standards, product docs and the research corpus are layer 3, loaded
only when a skill says to read one — never wholesale.

⚠️ **`.claude/` is an adapter layer and holds no procedures.** A command file
containing a procedure rather than a pointer is a fork waiting to drift.
`.claude/skills` is a symlink to `.agents/skills`.

## Non-negotiables

1. **One task equals one commit equals one change that leaves the tree green.**
   The commit subject starts with the backlog task ID.
   → `./gradlew checkCommitMessage`
2. **Never move a threshold in the direction that weakens its gate, and never
   delete a test, to make a check pass.** Cost budgets are thresholds too.
   *No script yet.*
3. **The test is written first and observed to fail; production code changes to
   satisfy the test, never the reverse.** New tests must have a red record.
   → `./gradlew checkTdd`, `./gradlew checkTestIntegrity`
   ⚠️ These raise the cost of skipping; they do not prove virtue — see rule 4.
4. **Never claim a test passes, a gate runs, or a number was measured, without
   having done it.** **No script enforces this, and none can.** Every other rule
   rests on it: a green gate reported by someone who did not run it is worth less
   than no gate. It matters more here than in a human-written project, because no
   human reads the code.
5. **Every commit is reviewed by an agent that did not write it, in two passes
   with two recorded verdicts** — `reviewer` for production, `test-reviewer` for
   the tests — given the task and the diff but never the author's reasoning, with
   each verdict bound to the staged diff by hash.
   → `./gradlew checkReviewed` (the review evidence producer remains a separate workflow)
   ⚠️ The test pass is separate because **test weakness is invisible to coverage**,
   which counts executed lines rather than constrained ones.
   ⚠️ **One agent runs both passes** (M0.114). Two agents re-read the same diff,
   packet and standards, which was the largest single cost of a round; the two
   questions, the two findings lists and the two verdicts are what the separation
   actually buys, and those are kept. What is lost is that an opinion formed in
   pass 1 travels into pass 2 — review.md rules 1c and 1d say so rather than
   pretending otherwise.
   ⚠️ The hash binds a verdict to a diff; it does **not** prove the reviewer was
   not the author — that is bought by the harness, and saying so is the same
   discipline as rule 3.
6. **Request rates scale with segments, AZs and nodes — never with records,
   shards, partitions or indices.** This one line is the architecture.
   See [cost.md](docs/internal/standards/cost.md). *No script until the store SPI
   lands with its counting decorator.*
7. **Business logic touches no socket, clock or object store directly.** If it
   needs I/O to test, it is in the wrong layer.
   → `./gradlew gates`
   ⚠️ It bans twelve I/O PACKAGES outright -- `java.nio.file`, `java.nio.channels`,
   `java.io`, `java.util.zip`, `java.util.jar`, `java.util.prefs`,
   `java.util.logging`, `java.sql`, `javax.sql`, `javax.naming`, `java.net`,
   `javax.net` -- so no sibling can
   be NAMED inside one. ⚠️ It does NOT close subclassing OUT of one:
   `java.util.jar.JarFile` extends the banned `java.util.zip.ZipFile` and passed
   until its own package was listed, so every package a reach can live in must
   be named. The byte, checksum and
   exception types living in them are named exceptions, which is what makes
   `java.io` bannable at all. The clock and the subprocess are banned by
   CONSTRUCT instead (`.now()`, `currentTimeMillis(`, `Clock.system`, `.exec(`),
   because their packages hold `Clock`, `Instant`, `List` and `Runtime` and
   cannot be forbidden -- taking a `Clock` parameter is the shape this rule
   REQUIRES, and `LocalDate.now(clock)` is how you read it.
   `binstore-backends` is exempt: it is the adapter. Three files are exempt by
   name: `server/.../Main.java` (real clock), `server/.../ConfigFile.java`
   (settings file), `server/.../NodeLocalStoreReaderMain.java` (reader process,
   secret and loopback socket), `client/.../HttpCatchUpExchange.java` (the JDK
   streaming HTTP response for catch-up; Helidon's blocking submit path waits
   for the complete body), `client/.../InstallationSecret.java` (owner-only
   secret-file adapter), and `client/.../NodeLocalStoreReaderClient.java`
   (loopback HTTP adapter). ⚠️ Files rather than the whole modules, so the
   remaining server composition stays under the gate and other consumer code
   cannot open sockets. The exemption is pinned by a case that scans exactly
   those six and fails if any stops reaching past its seam, so it cannot
   outlive its reason.
   ⚠️ **NEITHER HALF IS CLOSED.** The package half must name every package a
   reach can live in, and the construct half is a hand-named list; each is
   exactly as complete as its enumeration. It does not close this rule. It reads source text: reflection reaches any banned construct, a
   dependency's API can open a socket without naming one, and a helper inside
   the exempt module reaches the filesystem for a caller outside it. It raises
   the cost of reaching past a seam. The list it does enforce is in
   `RepositoryGateChecks.ioSeam` JVM predicate and every entry is pinned by a case, with the
   sample table checked against the list itself so neither can drift.
8. **A format change updates the format, every reader, every writer, the fakes,
   the golden files and the ADR in one commit.**
   See [`wire-format-change`](.agents/skills/wire-format-change/SKILL.md).
   *No script yet.*

9. **A new check is deterministic by default.** Before adding a gate, a review
   step or a research step, work down the ladder in
   [`gate-design`](.agents/skills/gate-design/SKILL.md): make the bad state
   unrepresentable, derive it from a source of truth, script it, commit the fact
   and diff it, generate it — and only then ask an agent. **If the rule can be
   stated as a predicate over files in the tree, an agent must not be asked to
   check it.** An instruction in a prompt is the weakest enforcement there is:
   it differs per run, is invisible outside the prompt, and dies with the
   session. ⚠️ Reaching for rung 6 or 7 means writing one sentence in the commit
   body saying why 1–5 cannot carry it.

## Gates

⚠️ **This section is the honest answer to "what actually runs".** Enforcement
is owned by Gradle tasks and the JDK; the historical files under `scripts/` are
not hook entry points.

<!-- index:gates:start -->
| Gradle task | Stage | Enforces |
|---|---|---|
| `./gradlew gates` | pre-commit | repository invariants plus wiring, override, portability, I/O-seam, metric, module, and dependency checks |
| `./gradlew checkHarnessTests` | pre-commit | JVM-native buildSrc gate tests |
| `./gradlew checkWired` | pre-commit | every M8 unwired-set entry is wired or owned by an open backlog row |
| `./gradlew checkOverride` | pre-commit | changed review override entries agree with recorded verdicts |
| `./gradlew checkReviewed` | pre-commit | staged review verdicts are bound to the staged diff |
| `./gradlew checkTdd` | pre-commit | newly added tests have byte-bound red evidence |
| `./gradlew checkTestIntegrity` | commit-msg | test removals or assertion weakening have a commit-body reason |
| `./gradlew checkCommitMessage -PcommitMessageFile=<file>` | commit-msg | commit subject names a real backlog task |
| `./gradlew checkMilestoneVerified -PmilestoneDir=<dir>` | manual | every acceptance criterion has an evidence line |
| `./gradlew checkCoverage` | manual | regenerated JaCoCo reports meet line and branch floors |
| `./gradlew checkSuiteTime -PsuiteLayer=L0 -PsuiteSeconds=<n>` | manual | measured suite time stays within its layer budget |
| `./gradlew checkMutants` | manual | native Gradle mutation-diff tasks run for modules that provide them |
| `./gradlew dependencyLicenses` | build/check | dependency SHA-1 pins, licence files, and denied licences |
| `./gradlew check` | build/check | runs the complete Gradle/JDK gate set and dependency licence gate |
<!-- index:gates:end -->

The historical shell/Python gate implementations are retained as migration
references only. The enforced pre-commit path is the Gradle/JDK task graph;
none of the hooks invokes a script interpreter.

Not yet existing, and named by skills and standards that say so: the cost meter
(`cost-budget`) and the benchmark gates. **When a skill tells you to run one of
these and it is absent, say the gate did not run.** Do not proceed as though it
passed.

⚠️ The staged-evidence tasks (`checkReviewed`, `checkTdd`, and
`checkTestIntegrity`) intentionally inspect the local index and local evidence
stores. CI must invoke them with the CI-specific diff/evidence setup; a clean
checkout with no staged diff is not evidence that those predicates were
checked.

⚠️ Repository gates examine the bounded checkout; staged-evidence tasks examine
only the staged diff by design. See [build.md](docs/internal/standards/build.md)
§ Gate scope for the distinction.

⚠️ **Known blind spot in `check-tdd` / `check-test-integrity`:** a test
annotated only with a project-defined *composed* annotation
(`@Test public @interface ClusterTest {}`, then `@ClusterTest void x()`) is not
seen by either gate. It does not refuse — it does not see the test at all, so no
red record is demanded and a weakening is invisible. Recorded as M0.17. Until it
lands, annotate tests with a JUnit annotation directly.

⚠️ **A second, distinct blind spot in `check-tdd`:** a test whose failure mode
is a JVM crash (an `OutOfMemoryError` under a deliberately small heap, for
example) does not produce a JUnit `<failure>` element — the test executor
process dies first, and the result is recorded as `<skipped/>`.
`tdd_scan.py record-one` reads only `<failure>`/`<error>`, so it reports the
test as never having failed, indistinguishable from one that passed. Found on
M1.18's `MemoryFlatUnderTenXBodySizeTest`: four mutations at different
magnitudes (fully disabled, 50x, 5x, 2x the real chunk size) all crashed the
executor rather than failing an assertion, so no red record could be produced
mechanically. Falsifiability was verified by hand instead (the mutation
genuinely and repeatably throws `OutOfMemoryError`), and the commit was made
with `SKIP=check-tdd`, stated plainly rather than worked around. No script
fix is proposed yet — unlike M0.17, closing this would mean teaching the
scanner to treat a crash-with-no-failure-element as a positive signal for
*this specific class* of test, which risks masking a genuinely-skipped test
in every other case.

⚠️ **So non-negotiable 5 is enforced locally only.** A commit made with
`--no-verify` carries no reviewer verdict and nothing downstream will notice.
The hash binds a verdict to a diff; it does not make the verdict travel. Closing
that would mean committing verdicts to the tree or checking them server-side,
and neither is built — so the honest statement is that this one rests on the
harness rather than on a gate.

## Never

- Never push unless asked.
- Never commit a tree you know is broken, including "I will fix it in the next
  commit".
- Never report to a person in bare task IDs. `M0.20` names nothing a reader can
  hold: say **`M0.20 (check-cross-refs.sh — every M<n> and R<n> resolves)`**.
  A status line built out of IDs — "M0.20, M0.28 and M0.9 are queued" — forces
  the reader to open `backlog.md` to learn what is being discussed, and reads as
  progress without being checkable. The name is a few words saying what the task
  *is*. ⚠️ **No script can enforce this**, because it governs what is said rather
  than what is committed; it holds only as long as it is followed.
  ⚠️ And the ID must RESOLVE. The first draft of this bullet taught the rule
  using `M0.37`, an ID with no backlog row — invented in conversation, repeated
  for a whole session, and never written down. An unresolvable ID is the same
  defect one step worse: it names nothing AND there is nothing to look up.
  `M0.20` is the gate that would catch it in the tree; nothing catches it in
  speech.
- Never widen scope silently. Doing more than the task asked is as much a problem
  as doing less, because it breaks the one-task-one-commit property.
- Never add an object-store request that scales with records, shards, partitions
  or indices.
- Never put object-store credentials in index settings.
