# 0039. A vacancy is found from the lease's expiry, not from a peer's refusal

Status: accepted
Date: 2026-09-10
Requirements: FR-11, NFR-1, NFR-9

## Context

`Leadership.sequencer()` elects into a vacancy on demand, and a vacancy can only
be found by asking the store: `LeaseManager.tryAcquire` issues a `stat` on the
lease key to learn whether it exists, then a `get` to read its expiry, and
returns empty. A pod that does not lead pays both **on every commit**, for the
whole time a healthy leader exists elsewhere — measured at **~13% of the
write-path bill** across cost.md R1b's interval range. ⚠️ The derivation is
recorded rather than asserted: 2 GET/s at R1b's 5 s ceiling with 6 pods is
≈ $2.1/mo against $15.55, and 40 GET/s at 250 ms is ≈ $41 against $311 — ~13% at
both ends. It is in `review/verdicts/M5.6d/3.reviewer.json`.

⚠️ **Those two requests cannot be made one.** Expiry is only in the body, and
`ObjectStat` deliberately carries no time. ⚠️ And the `stat` is not there to
detect absence — a `get` would notice that too, only indistinguishably from a
store failure — it is there because the take-over is a CONDITIONAL write and
only `stat` returns the `Version` that write needs. So the saving is
not in asking more cheaply. It is in not asking twice for something the pod is
about to read anyway: a forwarding pod already `get`s the lease to learn where
to send.

## Decision

**The vacancy check is answered from the EXPIRY of the lease the forward
already reads.** One `get` per follower commit serves both questions — is the
term free, and if not, where do I send — and an election runs only when that
lease is expired or absent.

⚠️ **AND WHEN THAT READ FAILS, FALL BACK TO THE FULL ELECTION** — the `stat`
plus `get` this decision exists to avoid. `BinStore.get` has no absence signal:
an absent lease key and an unreachable store arrive as the same plain
`IOException`, which is the same limitation that condemns the rejected
alternative below. Without the fallback, a fleet that boots while the store is
unwell has no lease object, every `Leadership` constructor swallows its failed
attempt, and after recovery every commit dies in the lease read with no pod
electing — the same "until a process restarts" outage, reached from the other
side. The fallback is safe because the election re-verifies everything from the
store; it costs two requests exactly when the cheap path could not answer.

⚠️ **AND IT MUST NOT MAKE A LEADER PAY.** A pod holding the term issues no lease
request today. The obvious refactor — hoist the lease read to the top of
`commitAll` and hand it to the forward — would give every leader commit one GET
it does not need. The read belongs on the path that forwards, not before the
branch.

⚠️ **This is the decision, not the implementation.** M5.6g owns writing it, and
it needs a clock in `FleetSequencer` and a way to hand `RemoteSequencer` a lease
already read rather than making it read a second one.

## Alternatives considered

**Promote on a peer's refusal instead: forward first, and elect only when the
pod the lease NAMES refuses and a re-read finds the lease unchanged.** This was
written, reviewed and **withdrawn**, and it is recorded here because it looks
strictly better — it removes the store request entirely rather than sharing it,
and the pair "refused, and the lease has not moved" reads like exactly the
vacancy the eager election was hunting for.

⚠️ **IT IS NOT THE SAME CONDITION, AND THE DIFFERENCE IS LIVENESS.** A refusal
requires a peer that is ALIVE AND ANSWERING: `SequencerTransport`'s javadoc
defines `NotTheLeaseholderException` as "when the peer **answers** that it does
not hold the lease", and says every other outcome is a plain, ambiguous
`IOException`. A leader lost to a node failure answers nothing. The send fails
plainly, the re-read arm is never reached, and **no pod elects again at any TTL
until a process restarts** — which is the precise case the election exists for.
Refusal and expiry are **disjoint**, not stronger and weaker: refusal covers a
live non-leading peer, expiry covers the holder that is gone.

⚠️ **AND THE TEST SUITE COULD NOT SEE IT.** `InProcessTransport` turns an
unknown endpoint into `NotTheLeaseholderException`, so every promotion test and
the commit-protocol simulation model a dead leader as `gone(endpoint)` — a
refusal. The whole promotion coverage rested on a fixture behaviour the
production transport (M5.6e, unwritten) is documented not to have. A change that
is green only because the fake answers the question under test is the shape this
project's own `InProcessTransport` javadoc warns about — "routing to whichever
sequencer we have would make every test pass regardless" — and it took a
reviewer tracing liveness state by state to find it.

⚠️ **Two more paths lose their recovery with it.** ADR-0027's self-fence becomes
permanent rather than TTL-bounded — the fenced pod refuses its own forward with
a plain `IOException`, and nothing re-checks expiry to end it — and an absent
lease key surfaces as a plain `IOException` too, so a cold or garbage-collected
lease never reaches an election either.

⚠️ **ADR-0012 cuts against it in the direction that matters.** "A peer hint may
accelerate, never decide" holds for the positive case — the store's conditional
write still decides who leads. But the ABSENCE of the hint would decide that no
election happens at all, and that is neither verifiable nor harmless.

**Elect on ANY forwarding failure.** Rejected outright: a transient 503 on the
lease read would burn an epoch, a seal and a full chain recovery, and
`FleetSequencer` already keeps its term through an ambiguous failure for exactly
that reason.

**Cache the last election's answer for a TTL.** Rejected because it delays
promotion by up to that TTL in the case the eager election handles immediately,
and because it is a second source of truth about leadership. ⚠️ Not rejected for
needing a clock: the accepted decision needs one too, and saying otherwise would
make the comparison flattering rather than true.

## Consequences

- A steady follower's commit should cost **one** object-store request once
  M5.6g lands, down from three.
- ⚠️ **Until it does, the eager election stands**, and so does its ~13%. The
  cost is known and priced; a liveness hole would not have been.
- ⚠️ **The promotion tests were weaker than they looked**: they modelled a dead
  leader as a refusing one, because `InProcessTransport` answers an unknown
  endpoint with a refusal. That is what let this ADR's rejected design pass a
  full green suite. ⚠️ **The fixture half is closed** — `unreachable(endpoint)`
  answers nothing, and
  `DeadPeerTest.aFollowerTAKESTheTermWhenTheLeaderIsDEADAndTheLeaseHasLAPSED`
  reds under the rejected design, verified by restoring it. It did not need
  M5.6e: `SequencerTransport.send`'s javadoc already decides what a dead peer
  looks like. ⚠️ **The fleet-level half is not**: the existing promotion tests
  and `CommitProtocolSimulation` still drive death as `gone(endpoint)`, and that
  is M5.6j.
- ⚠️ **A FIRST ATTEMPT AT THAT GUARD DID NOT GUARD**, and it is recorded because
  the mistake is easy to repeat: it asserted that a follower does NOT promote
  when a dead peer's forward fails, which is true under the rejected design and
  under this one. A test both worlds agree on guards neither. What separates
  them is a dead leader AND a lapsed lease — the term genuinely going begging
  with nobody alive to refuse anything.
