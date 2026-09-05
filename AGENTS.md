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

⚠️ **Do not read it wholesale and do not re-derive what it answers.** Route
through its README, or use the [`research`](.agents/skills/research/SKILL.md)
skill, which is where the rest of this lives: the revision banners that overturn
an earlier finding (**the banner wins**), and
[50-open-questions.md](docs/research/50-open-questions.md), where **every question
is answered** — ten as [ADRs](docs/internal/product/decisions/), six deferred to
*measurement*. ⚠️ Re-opening a settled decision is allowed; it is an ADR, not a
task.

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

Deprecated synonyms are **rejected at commit time**. → `scripts/check-terminology.sh`

## Standards

Rules that are always true. Read the one covering what you are touching. Each
names its gate, or is marked as having none — **the second kind matters more,
because it is where judgement is still required.**

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

⚠️ **A rule whose script is missing is a preference.** Most of these have none
yet — see *Gates* below for what actually runs today.

## Skills

Procedures, in [.agents/skills/](.agents/skills/README.md), written to the Agent
Skills spec. A skill calls a script in `scripts/`, never a tool-specific built-in.

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

**Progressive disclosure.** Layer 0 is this file, an index; layer 1 the skill
*descriptions* above; layer 2 a skill's body; layer 3 the standards, product docs
and research corpus, loaded only when a skill says to read one — never wholesale.
⚠️ `.claude/` is a thin adapter and holds no procedures.
→ [skills/README.md](.agents/skills/README.md).

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
4. **Never claim a test passes, a gate runs, or a number was measured, without
   having done it.** **No script enforces this, and none can.** Every other rule
   rests on it: a green gate reported by someone who did not run it is worth less
   than no gate. It matters more here than in a human-written project, because no
   human reads the code.
5. **Every commit is reviewed by two agents that did not write it** — `reviewer`
   for production, `test-reviewer` for the tests — given the task and the diff but
   never the author's reasoning, with the verdict bound to the staged diff by hash.
   → `scripts/review.sh`, `scripts/check-reviewed.sh`,
   [review.md](docs/internal/standards/review.md) — including why the test pass
   is separate, and what the hash does *not* prove.
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
   [`gate-design`](.agents/skills/gate-design/SKILL.md). **If the rule can be
   stated as a predicate over files in the tree, an agent must not be asked to
   check it** — an instruction in a prompt differs per run, is invisible outside
   the prompt, and dies with the session. ⚠️ Reaching for rung 6 or 7 means one
   sentence in the commit body saying why 1–5 cannot carry it.

## Gates

⚠️ **The honest answer to "what actually runs".** A skill or a standard may name
a script that does not exist; `scripts/` is the truth on the day you read it.
Generated from `.pre-commit-config.yaml` and `scripts/` — a hand-maintained list
of what runs is exactly the list that goes stale:

<!-- index:gates:start -->
| Script | Stage | Enforces |
|---|---|---|
| `check-portability.sh` | pre-commit | skills/README.md rules 2,3,5: skills are vendor-neutral and adapters are thin |
| `check-gate-scope.sh` | pre-commit | every gate judges this repository only, never .tmp/ or a sibling checkout |
| `check-harness-tests.sh` | pre-commit | the harness's own tests run -- buildSrc tests are NOT run by ./gradlew build |
| `check-module.sh` | pre-commit | architecture.md rules 2/4/5: each module stays inside its dependency surface |
| `check-session-load.sh` | pre-commit | what EVERY session loads stays bounded -- CLAUDE.md and its imports, plus an open-rows-only backlog |
| `check-rule-citations.sh` | pre-commit | every rule of review.md is cited by NAME, and every name resolves -- a number goes stale silently |
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
**`check-mutants.sh`** (80% killed on changed code), the cost meter
(`cost-budget`), the benchmark gates. **When a skill tells you to run one and it
is absent, say the gate did not run** — never proceed as though it passed.

⚠️ **Gates run in `delta` mode by default** — only the files a change touches —
and print which mode they used. `GATE_SCOPE=full` examines the whole tree and is
what CI runs. See [build.md](docs/internal/standards/build.md) § Gate scope.

⚠️ **What runs locally and what runs in CI are not the same list.** `check-tdd`
and `check-reviewed` **cannot** run in CI at all, and `check-reviewed` passes
*vacuously* rather than failing when it is invoked with nothing staged —
so **non-negotiable 5 is enforced locally only**, and a `--no-verify` commit
carries no verdict that anything downstream will notice. `check-tdd` has two
further blind spots that make it see nothing rather than refuse. All of it, with
what was measured: → [build.md](docs/internal/standards/build.md)
§ What CI can and cannot enforce.

## Never

- Never push unless asked.
- Never commit a tree you know is broken, including "I will fix it in the next
  commit".
- Never report to a person in bare task IDs, and never name an ID that does not
  RESOLVE. `M0.20` names nothing a reader can hold: say
  **`M0.20 (check-cross-refs.sh — every M<n> and R<n> resolves)`**. ⚠️ No script
  enforces this — it governs what is *said*, not what is committed.
  → [review.md](docs/internal/standards/review.md) § Reporting to a person.
- Never widen scope silently. Doing more than the task asked is as much a problem
  as doing less, because it breaks the one-task-one-commit property.
- Never add an object-store request that scales with records, shards, partitions
  or indices.
- Never put object-store credentials in index settings.
