---
name: wire-format-change
description: Change a persisted or on-the-wire format — the segment layout, the object key grammar, the commit-log delta or checkpoint, the subscription protocol, or the store SPI. Use whenever bytes that outlive a process, or cross a process boundary, change shape.
---

# Wire and format change

This project's real contracts are not Java interfaces. They are **bytes that
outlive the process that wrote them**, and bytes that cross between the ingester,
the plugin and the object store. Changing one is the highest-blast-radius edit
available here, because old bytes stay in the bucket for the whole retention
window and old peers stay running through a rollout.

## The five contracts

| Contract | Defined by | Read by |
|---|---|---|
| **Segment layout** | `docs/research/30-design-space/01-object-layout-and-format.md` | writer, reader, compactor, recovery |
| **Object key grammar** (incl. `h<headerLen>` and the membership filter) | `…/01` §5, `…/02` | LIST recovery, GC, the reader's one-request path |
| **Commit log** (delta, checkpoint, lease) | `…/03-metadata-and-cas.md` | sequencer, every reader's fallback, recovery |
| **Subscription protocol** (events, session/epoch, `via` modes, grants) | `…/04-discovery-and-tailing.md`, `…/10-client-library-and-fetch-modes.md` | service, consumer library |
| **Store SPI** (`BinStore`, `Version`, `Capabilities`) | `…/07-pluggable-store-abstraction.md` | every backend |

## The rule

⚠️ **One commit changes the format, every reader, every writer, the fakes, the
conformance suite, the golden files, and the ADR.** A format change split across
commits leaves the tree able to write bytes it cannot read.

## Before changing anything

1. **Is a version bump enough?** Every format carries a version or a tag byte
   for exactly this. Adding a field to a versioned struct is not a contract
   change; changing the meaning of an existing one is.
2. **What happens to bytes already in the bucket?** They live for the retention
   window (default 6 h) and forever in a stalled tenant. **A reader must handle
   the old shape until every possible writer of it has aged out.**
3. **What happens during a rollout?** Old and new pods coexist, and old plugins
   talk to new pods. Readers lag writers by at least one release: **ship the read
   side first, in an earlier commit, then the write side.**
4. **Does an unknown value mean "ignore" or "stop"?** Say so in the format doc.
   For the key filter, an unknown tag means *read the header*, never *no match* —
   getting that backwards silently drops data.

## Checklist

- [ ] Version/tag bumped, and the old shape still parses
- [ ] Every reader updated in the same commit
- [ ] Fakes and `MemoryBinStore` updated
- [ ] **Golden-file tests** for the old and the new shape — a format with no
      golden file is a format nobody will notice breaking
- [ ] Store conformance suite passes on **every** backend, not just local FS
- [ ] Round-trip property test (encode → decode → equals)
- [ ] Key-length bound re-checked against 1,024 bytes for the pathological input
- [ ] ADR written ([`adr`](../adr/SKILL.md)) and travelling in this commit
- [ ] The research document that defines the format updated, with a ⚠️ banner if
      a conclusion changed

## Cost re-check

A format change often moves a request count without anyone noticing: a bigger
header can push a speculative read over its guess, a reordered directory can
break coalescing, a new section can force a second GET. Run
[`cost-budget`](../cost-budget/SKILL.md) and state the before/after
requests-per-MiB in the commit body.
