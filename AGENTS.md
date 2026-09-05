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
- [docs/internal/product/backlog.md](docs/internal/product/backlog.md) — the current task list, authoritative. **Open rows only**, each a summary; landed rows are in [backlog-done.md](docs/internal/product/backlog-done.md) and the reasoning behind a long row is under its ID in [backlog-notes.md](docs/internal/product/backlog-notes.md). ⚠️ Read the notes for the ONE task you pick, never whole
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
→ `scripts/check-terminology.sh`

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
| Code | [code-structure.md](docs/internal/standards/code-structure.md) | Adding a module, package or file; when a file nears 500 lines; or when deciding where a seam belongs. |
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
   → `scripts/check-commit-msg.sh`
2. **Never move a threshold in the direction that weakens its gate, and never
   delete a test, to make a check pass.** Cost budgets are thresholds too.
   *No script yet.*
3. **The test is written first and observed to fail; production code changes to
   satisfy the test, never the reverse.** New tests must have a red record.
   → `scripts/tdd-red.sh`, `scripts/check-tdd.sh`, `scripts/check-test-integrity.sh`
   ⚠️ These raise the cost of skipping; they do not prove virtue — see rule 4.
4. **Never claim a test passes, a gate runs, or a number was measured, without
   having done it.** **No script enforces this, and none can.** Every other rule
   rests on it: a green gate reported by someone who did not run it is worth less
   than no gate. It matters more here than in a human-written project, because no
   human reads the code.
5. **Every commit is reviewed by two agents that did not write it** — `reviewer`
   for production, `test-reviewer` for the tests — given the task and the diff but
   never the author's reasoning, with the verdict bound to the staged diff by hash.
   → `scripts/review.sh`, `scripts/check-reviewed.sh`
   ⚠️ The test pass is separate because **test weakness is invisible to coverage**,
   which counts executed lines rather than constrained ones.
   ⚠️ The hash binds a verdict to a diff; it does **not** prove the reviewer was
   not the author — that is bought by the harness, and saying so is the same
   discipline as rule 3.
6. **Request rates scale with segments, AZs and nodes — never with records,
   shards, partitions or indices.** This one line is the architecture.
   See [cost.md](docs/internal/standards/cost.md). *No script until the store SPI
   lands with its counting decorator.*
7. **Business logic touches no socket, clock or object store directly.** If it
   needs I/O to test, it is in the wrong layer. *No script yet.*
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

⚠️ **This section is the honest answer to "what actually runs".** A skill or a
standard may name a script that does not exist; `scripts/` is the truth on the
day you read it.

Running today, wired in `.pre-commit-config.yaml`. ⚠️ **This table is
generated** from that file and from `scripts/` — a hand-maintained list of what
runs is exactly the list that goes stale:

<!-- index:gates:start -->
| Script | Stage | Enforces |
|---|---|---|
| `check-portability.sh` | pre-commit | skills/README.md rules 2,3,5: skills are vendor-neutral and adapters are thin |
| `check-gate-scope.sh` | pre-commit | every gate judges this repository only, never .tmp/ or a sibling checkout |
| `check-harness-tests.sh` | pre-commit | the harness's own tests run -- buildSrc tests are NOT run by ./gradlew build |
| `check-module.sh` | pre-commit | architecture.md rules 2/4/5: each module stays inside its dependency surface |
| `check-backlog-size.sh` | pre-commit | backlog.md holds open, summary-sized rows only -- it is read at every session start |
| `check-links.sh` | pre-commit | every relative markdown link resolves |
| `scripts/build-index.sh --check` | pre-commit | the generated index regions in AGENTS.md and skills/README.md are current |
| `check-terminology.sh` | pre-commit | glossary.md: one name per concept -- producer/ingester/writer/reader/consumer/plugin |
| `check-license-headers.sh` | pre-commit | build.md: every source file carries the SPDX Apache-2.0 header |
| `check-dependency-licenses.sh` | pre-commit | build.md: no GPL/AGPL/SSPL dependencies in an Apache-2.0 project |
| `check-metric-cardinality.sh` | pre-commit | observability.md rule 1: no high-cardinality metric or span labels |
| `check-test-budget.sh` | pre-commit | build.md: memory caps declared; a runaway dies as a JVM/Docker OOM, not a lost WSL2 session |
| `check-file-size.sh` | pre-commit | code-structure.md rule 1: no source file over 500 lines |
| `check-tdd.sh` | pre-commit | testing.md rule 2: every new test was observed to fail before the code existed |
| `check-reviewed.sh` | pre-commit | non-negotiable 5: the staged bytes were reviewed by both reviewers |
| `check-commit-msg.sh` | commit-msg | non-negotiable 1: the commit subject names a real backlog task |
| `check-test-integrity.sh` | commit-msg | testing.md rules 4-5: no assertion weakened alongside a production change |

Present in `scripts/` but **not** wired into `.pre-commit-config.yaml` — invoke by hand, from a skill, or from CI: `check-coverage.sh`, `check-milestone-verified.sh`, `check-suite-time.sh`.
<!-- index:gates:end -->

Not yet existing, and named by skills and standards that say so:
**`check-mutants.sh` (80% killed on changed code)**, the cost meter
(`cost-budget`), and the benchmark gates. **When a skill tells you to run one of
these and it is absent, say the gate did not run.** Do not proceed as though it
passed.

⚠️ **What CI can and cannot enforce**, because the difference matters more
than the claim:

| Gate | In CI? | Why |
|---|---|---|
| the ten text/build gates | ✅ | `pre-commit run --all-files` |
| `check-test-integrity` | ✅ **only with `CHECK_RANGE`** | it reads the *staged* diff, which is empty in a fresh checkout; `CHECK_RANGE=<base-ref>` makes it compare against the push or pull-request base. Without it it prints `ok` having examined nothing |
| `check-tdd` | ❌ **cannot** | `CHECK_RANGE` lets it find the new tests, but the red records live in `.harness/tdd/red.json`, which `.gitignore` excludes — so it fails in CI with "no red record" no matter the range. Verified: `CHECK_RANGE=HEAD~1 ./scripts/check-tdd.sh` exits 1 in a clean tree |
| `check-commit-msg`, `check-test-integrity` | ⚠️ **needs explicit invocation** | `pre-commit run --all-files` runs the pre-commit stage only and never fires commit-msg hooks |
| `check-module` | ✅ **only with `GATE_SCOPE=full`** | its default delta path selects modules from the *staged* diff, which is empty in a fresh checkout, so it would report "no module changed" having built nothing. CI sets `GATE_SCOPE=full` to build all eight |
| `check-reviewed` | ❌ **cannot** | its evidence lives in `.harness/review/`, gitignored and local to the machine that ran the review. ⚠️ It is a *pre-commit-stage* hook, so `--all-files` **does** invoke it — and with nothing staged it prints `ok nothing staged` and **passes vacuously**. It does not fail, which is worse: a green line that means nothing. CI runs `SKIP=check-reviewed` so the skip is visible in the log instead |

⚠️ **Gates run in `delta` mode by default** — only the files a change touches —
and print which mode they used. `GATE_SCOPE=full` examines the whole tree and is
what CI runs. A gate that cannot be sound on a delta either escalates itself
(`check-links` goes full whenever a file is deleted or renamed) or does not offer
the mode. See [build.md](docs/internal/standards/build.md) § Gate scope.

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
