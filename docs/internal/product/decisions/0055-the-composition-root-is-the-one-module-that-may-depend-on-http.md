# 0055. The composition root is the one module that may depend on `http`

Status: accepted
Date: 2026-09-18
Requirements: FR-1, FR-12, FR-13, NFR-9
Research: docs/research/30-design-space/08-failure-domains-and-resilience.md

## Context

architecture.md dependency rule 4 reads, in full, **"Nothing depends on
`http`."** It was written when the eight modules below the root were the whole
tree, and `scripts/check-module.sh` asserts it literally: any module whose
resolved dependencies name `project ':http'` fails.

M8.4 lands `main()`, and `main()` must start a listener. The three services that
listener carries — the producer's `POST /{index}/_bulk`, the peer's forwarded
`POST /ctl/commit` and the consumer's long poll on `GET /sub/...` — all live in
`http`. So the composition root has to name that module, and rule 4 as written
forbids it.

The rule is not wrong; it is under-stated. What it protects is named in the
gate's own failure message: *"Via `client` or `plugin` this puts Helidon in the
OpenSearch JVM."* That is a real hazard with a real cost — the plugin runs
inside someone else's process and its dependency surface is a liability
(architecture.md rule 2, ADR-0019) — and it is a hazard about what depends on
the depender, not about `http` being named at all.

⚠️ **The M8 SPEC had already decided this and the gate had not been told.** Its
design section says the root "depends on everything and nothing depends on it,
so the dependency surface `check-module.sh` enforces gains a leaf rather than a
cycle". This ADR exists because the rule it contradicts is written down as an
absolute, and re-opening a settled decision is an ADR rather than a task
(AGENTS.md).

## Decision

**Rule 4 becomes: nothing depends on `http` except the composition root, and
nothing depends on the composition root.**

`server` may name `http`. Every other module still may not, unchanged.

And `check-module.sh` gains the second half as a new assertion it did not make
before: **no module may depend on `server`**. The exemption is therefore
conditional on the property that makes it safe, checked on every commit, rather
than granted on a promise.

## Consequences

- Helidon reaches exactly two modules: `http`, which is the adapter, and
  `server`, which is a leaf no other module can reach. The path to the
  OpenSearch JVM runs `plugin` → `client` → `format`, and `client` is in
  `check-module.sh`'s `NO_CLOUD` set and touches neither.
- The gate is **stronger overall than the rule it relaxes**: it admits one named
  module and adds a check nobody was making. A future module that depended on
  `server` to "reuse the wiring" would recreate exactly the cycle rule 4 was
  written against, and would now fail.
- ⚠️ **This is a relaxation, and non-negotiable 2 says a threshold must never
  move in the weakening direction to make a check pass.** The argument that it
  is not that: the rule's stated purpose is preserved intact, the relaxation is
  to one module named in the script rather than to a pattern, and it is paid for
  with a new assertion. A reader who disagrees should read the leaf check as the
  price, not as decoration.

## Alternatives rejected

**Put `main()` in `http`.** Rejected by the M8 SPEC with its reason: `http` is
the thin adapter and the in-process API is the primary one, so a root there
makes the HTTP surface the assembly point and hands every in-process embedder a
web server.

**Give `server` its own copy of the three services.** A second implementation of
the front door is precisely what ADR-0019's "two front doors, one
implementation" forbids, and the copy would drift on the first error-mapping
change.

**Invert it: have `http` expose a `start(...)` the root calls without naming the
module.** It does not exist — the root would still depend on `http` to call it.
Moving the routing into `http` behind one entry point is a reasonable later
refactor and changes nothing about which module names which.
