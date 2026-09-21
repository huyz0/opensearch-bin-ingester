# 0045. jzap replaces PIT as the mutation-testing engine

Status: accepted
Date: 2026-09-15
Requirements: none — a delivery-process decision under testing.md rule 9
  ("mutation score is the metric that measures whether tests constrain
  anything"), not a product FR/NFR
Research: none — a build-tooling decision, not a design one; the corpus does
  not cover mutation-testing engines

## Context

M0.14 introduced PIT as this project's mutation-testing engine and wired it as
a plain `JavaExec` (`pitest-command-line:1.21.1` + `pitest-junit5-plugin`),
deliberately not through `gradle-pitest-plugin`: that plugin targets Gradle 8
against this build's 9.7, and a plugin incompatibility would take the whole
gate with it. `1.21.x` was recorded as **a floor, not a preference** —
`pitest-entry` shades its own copy of ASM, so an external
`org.ow2.asm:asm:9.10.1` on the classpath changed nothing, and `1.19.1`'s
bundled copy threw `IllegalArgumentException: Unsupported class file major
version 69` (JDK 25) before generating a single mutant.

`check-mutants.sh`, the gate that would consume PIT's report, is still `todo`
on `main` (M0.14) — it exists on the abandoned `archive/m4` branch only. No
gate depends on PIT's output today; only the bare Gradle task does. That is
what makes this the moment to decide the engine, not the moment after the gate
already assumes one.

jzap (`huyz0/jzap`) is a same-author mutation-testing engine for Java and
Kotlin, Apache 2.0, first released (0.1.0) the same day this decision was
made. Its own compatibility page states: "Class files are read with ASM 9.10,
which understands class file versions through JDK 25" — tested, not merely
claimed — and its Gradle plugin's own compatibility page states "Gradle 9.7,
configuration cache compatible" — the exact version this build runs, and the
exact property `gradle-pitest-plugin` was rejected for lacking.

**Trying it surfaced a real defect, not a hypothetical one.** Applying
`io.github.huyz0.jzap` through the shared convention plugin broke
configuration for every module reachable through
`binstore-backends`'s `testImplementation(testFixtures(project(":binstore-spi")))`
— `IllegalStateException: Value for :binstore-spi project components has not
been calculated yet` — on every build, not only a mutation one, because the
plugin's aggregate-wiring code (`JzapPlugin.contributeToAggregate`) resolved a
sibling project's test classpath eagerly inside `Project.afterEvaluate`, which
orders only relative to the project it fires on. Filed and fixed upstream the
same day (`huyz0/jzap@b90dc7e`), released as `0.1.1`, and reverified against
this tree's own module graph before this record was written — `:sequencer`,
`:binstore-backends` and `:format` all configure and run `mutationTestDiff`
successfully against the real published `0.1.1`, not a local patch.

## Decision

Replace the `pitest` `JavaExec` task in
`buildSrc/src/main/kotlin/io.github.huyz0.os.biningester.java-conventions.gradle.kts` with the jzap
Gradle plugin (`io.github.huyz0.jzap`), applied through the same shared
convention so every module gets `mutationTest`, `mutationTestDiff` and (on the
root project, unused today since root applies no convention plugin here)
`mutationTestAll`. Both the plugin (`buildSrc/build.gradle.kts`) and the
analysis engine (`jzap { engineVersion = "0.1.1" }`) are pinned to `0.1.1`,
the first version that configures cleanly against this repository's module
graph — not `0.1.0`, which does not.

`check-mutants.sh` (M0.14, still `todo`) will drive `mutationTestDiff` rather
than hand-rolling the `-PmutantTargets`/`--targetClasses` class-list scoping
the removed `pitest` task needed: the plugin's diff mode (`--from`/`--to`,
defaulting to `HEAD..-Local-` — exactly a pre-commit gate's staged-and-
unstaged scope) replaces that machinery rather than sitting next to it.

No `threshold` or `failOnSurvivors` is set in the `jzap {}` block, matching
the removed PIT task's own restraint (it had no `--mutationThreshold`
either): pass/fail is `check-mutants.sh`'s decision to make from the report,
not this task's.

## Alternatives considered

**Keep PIT.** Rejected on the same evidence that chose `1.21.1` over `1.19.1`
in the first place: PIT's ASM support is shaded and bundled, so a future JDK
bump risks the identical failure recurring, discoverable only by hitting it
again. jzap's ASM dependency is a plain, unshaded `org.ow2.asm:asm:9.10.1`
(`jzap-gradle/build.gradle.kts`), the same version this project's own PIT
comment names as the one that reads JDK 25 — checkable in jzap's source
rather than inferred from a release note. Against that: PIT is the incumbent,
with a `1.21.1` pin already proven to work on this tree, and jzap at the time
of this decision was hours old (`0.1.0` → `0.1.1` same day). The defect this
record documents is exactly the risk of adopting software that young; it
surfaced immediately rather than in three months, which is the outcome this
project's own testing standards exist to produce, not evidence against
adopting it.

**Wait for a more mature jzap release before switching.** Rejected for what
it would cost, not what it would avoid: `check-mutants.sh` is not built yet,
so nothing depends on the engine choice today except this file and the bare
Gradle task — the cheapest possible moment to change it is now, before a gate
script encodes assumptions about either tool's report format. Waiting adopts
PIT's shaded-ASM risk by default for no benefit, since the eventual switch
would face the identical integration work this record already did.

**Run both engines and compare mutation scores before committing.** Not done
formally: `:sequencer:mutationTestDiff` against the current diff analysed
zero mutants in scope on both engines (the staged changes at the time carried
no mutable line), so no comparison run produced a score to compare. The
verification that was done instead — jzap's own test suite plus a live
`mutationTestDiff` run against the real published `0.1.1` on three of this
repository's own modules, including the one that reproduces the
`testFixtures` defect — establishes that jzap configures and runs correctly
against this tree, which is the question this ADR answers. A scored
side-by-side belongs to whichever commit first gives `check-mutants.sh` a
real diff to mutate.

## Consequences

**Easier:** `check-mutants.sh` inherits diff-scoping for free instead of
building it (`-PmutantTargets` never has to be invented for jzap). Adding a
JDK is no longer a PIT-shaded-ASM gamble — jzap's own compatibility page is
the thing to check, and it is testable in a way "which version does the
bundled fork use" is not.

**Harder:** jzap is a single-author, single-day-old project as of this
decision (`0.1.0`, `0.1.1` same day) — no second implementation exists to
cross-check its verdicts against, and this project has no relationship with
its maintainer other than being an early adopter. `docs/status.md` in jzap's
own repository lists real, stated gaps (test classes never mutated, git
renames not followed in diffs, line-level rather than block-level coverage)
that were not independently reverified here.

**Foreclosed, for now:** a scored comparison against PIT's own baseline
mutation numbers on this codebase, since PIT was never run to completion here
either (`check-mutants.sh` predates both engines' use on `main`). Whichever
engine `check-mutants.sh` is built against first sets the number every
future mutation-score claim on this project is compared to.
