---
name: next-task
description: Choose what to work on next and confirm it is genuinely ready. Use at the start of any working session, after finishing a task, or when unsure what to do. Prevents starting work that is blocked, unspecified, or already done.
---

# Next task

## Choose

1. Read `docs/internal/product/backlog.md`. It holds OPEN rows only, each a
   one-line summary — landed rows are in `backlog-done.md` and nothing loads it.
   ⚠️ **Read `backlog-notes.md` only for the task you pick**, following that
   row's `[notes]` link; loading it whole is the thing the split undid. Only the
   current milestone is decomposed; if it is not, decompose it before writing
   code ([`spec`](../spec/SKILL.md)).
2. Take the **top unblocked task** — top, not the most interesting one. Order is
   dependency order and was chosen deliberately.
3. A task is **blocked** if it depends on an unfinished task or on a decision
   nobody has made. ⚠️ **If the top three are all blocked, stop and report** —
   that is a planning problem, not a work problem.

## Confirm before starting

- **Does it cite a requirement?** A task serving no FR/NFR is unjustified work.
- **Does it have acceptance criteria, and are they checkable by something other
  than an opinion?** If not, write them first.
- **Is it re-opening a settled decision?** Every research question is answered
  ([`50-open-questions.md`](../../../docs/research/50-open-questions.md)); ten are
  ADRs. Contradicting one is allowed, but it is an ADR of its own, not a task.
- **Does it depend on a constant that needs measurement?** Six are listed under
  *deferred to measurement*. ⚠️ The answer is to build the thing that measures it,
  never to defer the design.
- **Is it one commit's worth?** One coherent change leaving the tree green. If
  not, split it now rather than discovering it half-way.
- **Has it already been done?** Check the backlog state and `git log`.

## Then

Implement with [`tdd`](../tdd/SKILL.md). Do not start editing before the
acceptance criteria exist — that is how scope drifts.
