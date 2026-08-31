---
name: skill-forge
description: Create, audit or edit a skill. Use when a procedure has been explained more than twice, when a skill is not being invoked at the right moment, or when adding a command adapter.
---

# Skill forge

## When a skill is warranted

A procedure explained more than twice, or a sequence with a step that is easy to
skip and expensive to skip. Not for one-off explanations — those belong in a
standard or the research corpus.

## Anatomy

```
.agents/skills/<name>/SKILL.md      the procedure — vendor-neutral, required
.agents/skills/<name>/references/   detail loaded only when the body says to
.claude/commands/<name>.md          a thin adapter: a pointer, never a procedure
```

Frontmatter is exactly `name` and `description`, both required. `name` must
equal the directory name. → `scripts/check-portability.sh`

## The description is the whole mechanism

⚠️ Layer 1 — every skill's `description` — is the **only** thing an agent sees
before deciding whether to load a skill. It must say **when to use this**, not
what it is.

- ❌ "Write an ADR" — a title
- ✅ "Use when making a choice that is expensive to reverse" — gets it loaded at
  the right moment

Write it as: *what it does. Use when `<concrete trigger>`.* Concrete triggers
are the words a user or an agent would actually be thinking at that moment.

## Rules

1. **A skill is a procedure, not an explanation.** Rationale lives in the
   standards and the research corpus; the skill says what to do and links to why.
2. **Skills call scripts in `scripts/`, never a tool-specific built-in.** The
   script is the enforcement path and must work for anyone on any tool.
3. **No vendor-specific syntax in `AGENTS.md` or any `SKILL.md`.** Claude Code's
   `@import` belongs in `CLAUDE.md`, which is the adapter.
4. **Adapters stay thin.** A `.claude/commands/*.md` containing a procedure
   rather than a pointer is a fork waiting to drift.
5. **Never claim a script exists that does not.** If a skill names a gate that
   is absent, the honest response when running it is to say the gate did not run
   — not to proceed as though it passed.

## After adding or renaming a skill

Run `scripts/build-index.sh` to regenerate the skill table in `AGENTS.md` and
`.agents/skills/README.md`, then `scripts/check-portability.sh`.

## Auditing an existing skill

- Is its description a trigger, or a title?
- Has it accumulated explanation that belongs in a standard?
- Does it name a script that exists?
- Would an agent with only the description know when to reach for it?
- Is it ever actually invoked? A skill nobody loads is worse than none — it
  costs layer-1 tokens every session and returns nothing.
