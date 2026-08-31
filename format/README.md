# format

Pure encode and decode: segment layout, the key grammar, membership filters and
commit-log records. No I/O, no clock, no configuration.

**Depends on:** nothing, and it must stay that way. This is where T0 tests live,
and its purity is what makes them cheap enough to run on every commit.

A change to anything here is a wire-format change: see
[`wire-format-change`](../.agents/skills/wire-format-change/SKILL.md).
