---
name: gate-design
description: Choose the cheapest reliable mechanism for a new check. Use when adding a gate, a review step, a research step, or any rule an agent is expected to follow — and before writing an instruction that says "remember to" or "make sure you".
---

# Designing a check

⚠️ **An agent instruction is the weakest possible enforcement.** It costs tokens
on every session, is invisible to anyone not reading the prompt, produces a
different answer on each run, and **dies with the session that wrote it**. Reach
for it last, not first.

## The ladder

Work down it. Stop at the first rung that can carry the rule.

| # | Mechanism | Why it beats the one below |
|---|---|---|
| **1** | **Make the bad state unrepresentable** | Nothing to check. Two copies of a parser drifted apart three times; one shared parser cannot. |
| **2** | **Derive from a source of truth** | `workspace_files` is git-derived, so a reference clone is out of scope *by construction* — no exclusion list to forget. |
| **3** | **Deterministic gate** (script or build task) | Same answer every run, for everyone, in CI, in milliseconds. Cacheable. |
| **4** | **Commit the fact, diff the change** | A pinned sha and a licence file make accepting a dependency a diff a human reads, not a resolve nobody sees. |
| **5** | **Generate, don't maintain** | A hand-written list of what runs goes stale. `build-index.sh` regenerates AGENTS.md § Gates from `.pre-commit-config.yaml`. |
| **6** | **Agent review** | Only for what no predicate can express. |
| **7** | **An instruction in a prompt** | Only when nothing above applies, and say so out loud. |

## The test

**Can you state the rule as a predicate over files in the tree?**
If yes, it is rung 3 or better and an agent must not be asked to do it.

- "no source file over 500 lines" → predicate → script
- "every dependency has a pinned sha" → predicate → build task
- "no gate walks the filesystem" → predicate → script
- "the diff does what the task says" → **not** a predicate → agent
- "would this test fail if the code were wrong" → **not** a predicate → agent

## When an agent is genuinely far superior

Reserve rung 6 for judgement that needs intent reconstructed:

1. **Task-versus-diff mismatch.** The task says X, the diff does Y. No script has the task in its head.
2. **Would this test catch a bug?** Mutation score approximates it; naming the surviving mutation does not.
3. **Is this request rate scaling with the wrong thing?** Requires reading the design, not the syntax.
4. **Is this argument sound?** Research, ADRs, weighing alternatives.

Everything else an agent is asked to "check" is a script someone has not written yet.

## ⚠️ The failure mode this exists to prevent

**A check that cannot fail deterministically tends to report success while
checking nothing.** Every instance found in this repository so far was of that
shape:

- a licence gate that grepped SPDX ids against a report emitting licence *prose*,
  so `GPL-2.0` could never match — **passing everything**
- a memory budget that summed a key Gradle does not read, so a daemon was
  unbounded while the gate counted 512 MiB for it
- two gates that printed `FAIL` and exited `0`
- a test-first gate whose parser lost a whole file to one `'}'` char literal, then
  printed `ok no new tests in this diff`

None was caught by an instruction telling an agent to be careful. Each was caught
by running the thing and each was fixed by moving it **up** the ladder.

## Before you add one

1. **Name the rung.** If it is 6 or 7, write one sentence saying why 1–5 cannot
   carry it. That sentence belongs in the commit body.
2. **Make it fail first.** Construct the input it must reject and watch it
   reject that input. A gate never observed failing is not known to gate anything
   — the same rule as [`tdd`](../tdd/SKILL.md), for the same reason.
3. **Check for a false-refusal path.** A gate that rejects legitimate input gets
   switched off, and then it enforces nothing. Run it over real material.
4. **Wire it in**, or it is a preference. `.pre-commit-config.yaml`, or a task
   `check` depends on.
5. **State what it cannot see.** A documented blind spot is worth more than an
   undocumented mechanism that half works.

→ [`docs/internal/standards/review.md`](../../../docs/internal/standards/review.md)
for what the agent reviewers are given and denied.
→ `AGENTS.md` § Gates is generated, so adding a hook updates it — do not hand-edit.
