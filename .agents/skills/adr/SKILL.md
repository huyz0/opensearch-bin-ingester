---
name: adr
description: Write an architecture decision record. Use when making a choice that is expensive to reverse, when changing a wire format or the store SPI, when a research conclusion is overturned, or when a future reader would otherwise ask "why on earth is it done this way".
---

# Architecture decision record

An ADR captures **why**, at the moment the alternatives are still live. Six
months later the code shows what was chosen and nothing shows what was rejected
— and the rejected options are the part people actually need, because they are
what stops the same debate reopening.

## When

- The choice is **expensive to reverse** — a wire format, the segment layout,
  the commit protocol, a dependency at the centre of things.
- It **changes a wire format or the store SPI**. Required, not optional, and it
  travels in the same commit as the change
  ([`wire-format-change`](../wire-format-change/SKILL.md)).
- It **overturns a research conclusion**. The corpus records what we believed;
  the ADR records that we stopped believing it and why. Also add a ⚠️ revision
  banner to the affected research document.
- It **moves a cost budget**. Budgets are the product.
- A future reader would ask "why on earth".

Not for choices that are cheap to undo. An ADR for everything is an ADR for
nothing.

## Format

`docs/internal/product/decisions/NNNN-short-title.md`:

```markdown
# NNNN. Title

Status: proposed | accepted | superseded by NNNN
Date: YYYY-MM-DD
Requirements: FR-n, NFR-n
Research: docs/research/<path>#section   (if it confirms or overturns one)

## Context
What forced a decision. The constraints, with numbers where they exist.

## Decision
What was chosen, stated so someone can act on it.

## Alternatives considered
Each one, and **why it was rejected**. This section is the reason the file exists.

## Consequences
What this makes easy, what it makes hard, and what it forecloses.
```

⚠️ **An ADR listing no rejected alternative is a description, not a decision.**
If nothing else was seriously considered, either say so explicitly — that is
information — or reconsider whether this needed an ADR.

⚠️ **In this project, quantify.** Nearly every real decision here has a number
attached (requests per MiB, bandwidth amplification, key bytes, latency). An
alternative rejected without its number is an assertion, and this project's
history is full of conclusions that flipped when the number was computed at a
different scale.

## Numbering

Sequential, never reused. Superseding does not delete: mark the old one
superseded and link forward, because the reasoning that was later overturned is
often the most instructive thing in the directory.

## Staleness

When the tree diverges from an accepted ADR without overturning the decision,
record it as a **dated note on the `Status:` line** — never by rewriting the
body. A body rewrite erases what was believed at decision time.
