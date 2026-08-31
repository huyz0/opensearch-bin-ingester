# Skills

Procedures, written to the [Agent Skills](https://agent-skills.org) spec so any
tool that reads `SKILL.md` can use them. Claude-specific files under `.claude/`
are **thin adapters that delegate here** — they never contain logic of their own.

## Progressive disclosure

This repository already holds a 21-document research corpus compiled before any
code existed. Loading it into every session is impossible and would be useless
if it were possible. The answer is four layers, each loaded only when the one
above says it is relevant.

| Layer | What | Loaded |
|---|---|---|
| **0** | [`AGENTS.md`](../../AGENTS.md) — an index, deliberately | every session |
| **1** | The `description:` line of every skill below | every session (a few hundred words total) |
| **2** | A skill's body | when that skill is invoked |
| **3** | Standards, product docs, the research corpus | when a skill says to read one |

⚠️ **Layer 1 is the whole mechanism.** A skill's `description` is the only thing
an agent sees before deciding to load it, so it must say *when to use this*, not
*what this is*. "Write an ADR" is a title; "use when making a choice that is
expensive to reverse" is a description that gets the skill loaded at the right
moment.

**Layer 3 is never loaded wholesale.** `docs/research/` has a README with a tier
table and a task-based routing table; the [`research`](research/SKILL.md) skill
exists to teach navigating it rather than reading it.

## The rules

1. **Skills call scripts in `scripts/`, never a tool-specific built-in.** The
   script is the enforcement path and it must work for a developer on any tool.
2. **`SKILL.md` frontmatter is `name` and `description`, both required**, `name`
   equals the directory name, and the file parses as YAML front matter followed
   by Markdown. A tool that cannot parse it ignores the skill silently.
3. **No vendor-specific syntax in `AGENTS.md` or in any `SKILL.md`.** Claude
   Code's `@import` belongs in `CLAUDE.md`, which is the adapter.
   → `check-portability.sh`
4. **A skill is a procedure, not an explanation.** Rationale lives in the
   standards and the corpus; the skill says what to do and links to why.
5. **Adapters stay thin.** A `.claude/commands/*.md` file that contains a
   procedure rather than a pointer is a fork waiting to drift.
   → `check-portability.sh`

## The skills

<!-- index:skills:start -->
| Skill | Use when |
|---|---|
| [`adr`](adr/SKILL.md) | When making a choice that is expensive to reverse, when changing a wire format or the store SPI, when a research conclusion is overturned, or when a future reader would otherwise ask "why on earth is it done this way" |
| [`bench`](bench/SKILL.md) | When a change touches a hot path, when adding or changing a JMH benchmark, when a latency or throughput number is claimed, or when tempted to optimise anything |
| [`cost-budget`](cost-budget/SKILL.md) | Whenever a change touches how objects are written, read, listed, or discovered — and before claiming any change is cost-neutral. This project exists to control these numbers |
| [`gate-design`](gate-design/SKILL.md) | When adding a gate, a review step, a research step, or any rule an agent is expected to follow — and before writing an instruction that says "remember to" or "make sure you" |
| [`milestone-review`](milestone-review/SKILL.md) | When a milestone reaches its completion condition or a checkpoint, and the commits need reading together rather than one at a time |
| [`milestone`](milestone/SKILL.md) | When told to work through a milestone, or when a session should keep going until the milestone's completion condition is met |
| [`next-task`](next-task/SKILL.md) | At the start of any working session, after finishing a task, or when unsure what to do. Prevents starting work that is blocked, unspecified, or already done |
| [`research`](research/SKILL.md) | Before any web search or design argument about object storage, cost models, OpenSearch ingestion, WarpStream/AutoMQ, CAS coordination, or Java runtime choices — the answer is usually already here, with numbers |
| [`review`](review/SKILL.md) | Before every commit. Defines what the reviewer is given, what it is deliberately denied, and what to look for that the deterministic gates cannot see |
| [`skill-forge`](skill-forge/SKILL.md) | When a procedure has been explained more than twice, when a skill is not being invoked at the right moment, or when adding a command adapter |
| [`spec`](spec/SKILL.md) | When starting a milestone, when a task lacks acceptance criteria, or when what to build is clearer than how it will be checked |
| [`tdd`](tdd/SKILL.md) | When writing any code. Covers the red-green cycle, what to assert, the test tiers, and the rules that keep the resulting test worth having |
| [`wire-format-change`](wire-format-change/SKILL.md) | Whenever bytes that outlive a process, or cross a process boundary, change shape |
<!-- index:skills:end -->

## ⚠️ When a skill names a script that is not there

**Several do, deliberately.** This project has a harness before it has a build,
so gates that need Gradle, JMH or a running service are named by skills and do
not exist yet. `AGENTS.md` § *Gates* lists exactly which scripts exist today, and
`scripts/` is the answer that is true on the day you read it.

The rule that matters: **a skill that says "run the gate" when the gate is absent
describes an intended step rather than an available one, and the honest response
is to say the gate did not run** — not to proceed as though it passed. Same
discipline as `AGENTS.md` non-negotiable 4 — never claim a gate ran without having
run it — and no script can check that one.

⚠️ **Do not add a list of missing scripts here.** It is a second copy of a fact
that moves, and in the repository this harness was adapted from that exact list
went stale four times. Check `scripts/` for the file.
