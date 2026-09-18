// SPDX-License-Identifier: Apache-2.0

// Everything every module of this project agrees on: the toolchain, the test
// tiers, and the memory caps. Applied by each module's build.gradle.kts, which
// then declares only its own dependencies.
//
// Rationale: docs/internal/standards/build.md and java-style.md.

import java.time.Duration

plugins {
    `java-library`
    jacoco
    `jvm-test-suite`
    // testing.md rule 19: the store conformance suite runs against every
    // backend, so it needs a home shared by T1 and T3 rather than a copy each.
    `java-test-fixtures`
    // ADR-0045: replaces the hand-rolled `pitest` JavaExec task below with the
    // plugin the PIT comment it replaces explicitly rejected -- that rejection
    // was about `gradle-pitest-plugin` targeting Gradle 8 against this build's
    // 9.7; this plugin's own compatibility page states Gradle 9.7, configuration
    // cache compatible, tested, which is the same claim measured rather than
    // taken on trust. ⚠️ NO VERSION HERE: a precompiled script plugin resolves
    // this from buildSrc's own compile classpath, not from a `plugins{}`
    // version -- Gradle refuses one with "Plugin requests from precompiled
    // scripts must not include a version number". The version is pinned once,
    // in buildSrc/build.gradle.kts, the same way every other buildSrc
    // dependency is.
    id("io.github.huyz0.jzap")
}

// JDK 25 LTS. OpenSearch 3.8 bundles 25.0.3+9, so one toolchain serves both the
// ingester and the plugin -- java-style.md rule 1. JEP 491 removed the
// `synchronized` pinning that made JDK 21 the wrong target for a service built
// on virtual threads.
java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val libs = extensions.getByType<VersionCatalogsExtension>().named("libs")
// Resolved once, at configuration time. Reading `layout` from inside a task
// action instead would capture the Project, which the configuration cache
// cannot serialise -- and a build that silently drops its configuration cache
// is a build nobody notices getting slower.
val scratch = layout.buildDirectory.dir("tmp").get().asFile.also { it.mkdirs() }

// testing.md rule 19: the store conformance suite is ONE suite run against every
// backend, so its base class lives in testFixtures and each tier subclasses it.
// ⚠️ Applying `java-test-fixtures` alone does not deliver that. The fixtures
// source set gets no test framework, so a @Test-bearing base class fails with
// "package org.junit.jupiter.api does not exist"; and a registered suite cannot
// see fixtures without an explicit `testFixtures(project())`. Advertising the
// capability without wiring it is how an author reaches for the cheap exit --
// a copy of the suite per tier, which is exactly what rule 19 forbids, and the
// copies drift until a CAS-rejection probe is asserted in one and dropped in
// the other.
dependencies {
    "testFixturesImplementation"(platform(libs.findLibrary("junit-bom").get()))
    "testFixturesImplementation"(libs.findLibrary("junit-jupiter").get())
    "testFixturesImplementation"(libs.findLibrary("assertj-core").get())
}

// testing.md rule 6: 95% line / 90% branch per module, measured from JaCoCo's
// XML by scripts/check-coverage.sh. ⚠️ XML is what the gate reads; HTML is off,
// because a gate that parses HTML is a gate that breaks on a tool upgrade.
// ⚠️ 0.8.13 is the FLOOR, not a preference: 0.8.12 cannot read Java 25 bytecode
// and fails with "Unsupported class file major version 69" while producing a
// zero-byte XML report. Measured against a real class in this tree.
jacoco { toolVersion = "0.8.13" }

// ⚠️ `check` must produce the report, not merely the exec data. Without this
// `./gradlew check` ran `test` and stopped, so the coverage gate read a report
// that existed only if someone had run a manual step -- and, once run, never
// went stale-detected. Review reproduced a module reporting ok 100% with an
// untested class compiled after the report was written.
tasks.named("check") { dependsOn(tasks.withType<JacocoReport>()) }

tasks.withType<JacocoReport>().configureEach {
    // The report is only meaningful after the tests that produce its exec data.
    mustRunAfter(tasks.withType<Test>())
    reports {
        xml.required.set(true)
        html.required.set(false)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-Xlint:all,-serial,-processing")

    // build.md: the compile daemon's heap is set HERE, because
    // `org.gradle.java.compile-daemon.jvmargs` in gradle.properties is not a
    // key Gradle reads -- the string does not occur anywhere in the 9.7.0
    // distribution. It was declared, counted in the budget, and enforcing
    // nothing: exactly the failure build.md is written against.
    options.isFork = true
    options.forkOptions.memoryMaximumSize = "512m"

    // -Werror because a warning nobody fails on is a warning nobody reads --
    // but on PRODUCTION code only. A test legitimately exercises a deprecated
    // path or ignores a resource, and -Werror there pushes an author toward a
    // weaker test or toward switching the lint off for everyone.
    if (name == "compileJava") {
        options.compilerArgs.add("-Werror")
    }
}

// The three tiers of docs/internal/standards/testing.md map onto three Gradle
// tasks, and only the first is wired into `check`:
//
//   ./gradlew test             T0-T2, no container      every commit
//   ./gradlew integrationTest  T3,    MinIO             on demand + CI
//   ./gradlew clusterTest      T4,    OpenSearch        on demand + CI
//
// The default task starting no container is the point. A developer, or an agent
// running /milestone, gets a fast light loop; the heavy tiers are explicit.
testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            useJUnitJupiter(libs.findVersion("junit").get().requiredVersion)
            dependencies {
                implementation(libs.findLibrary("assertj-core").get())
            }
            targets.configureEach {
                testTask.configure {
                    // build.md: set where the runtime enforces it, so a runaway
                    // dies as a JVM OutOfMemoryError rather than as WSL2 killing
                    // an unrelated session.
                    maxHeapSize = "512m"
                    jvmArgs("-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=$scratch")
                    // A hung test holds its memory until something reclaims it.
                    timeout.set(Duration.ofMinutes(10))
                    // testing.md rule 17: scratch under build/tmp, never the
                    // system temp directory. Enforced here so it is automatic.
                    systemProperty("java.io.tmpdir", scratch.absolutePath)
                    // ⚠️ FORWARDED EXPLICITLY, because a Gradle CLI `-D` sets it
                    // on the DAEMON and never reaches the test JVM. M4.13's
                    // sweep advertised `-Dsweep.seeds` as a knob and review
                    // measured it resolving to null in the fork -- a
                    // configurable seed count that was not configurable, and a
                    // claim already written into the archive row.
                    providers.systemProperty("sweep.seeds").orNull
                        ?.let { systemProperty("sweep.seeds", it) }
                    testLogging { events("failed") }
                }
            }
        }
        // The built-in `test` suite already sees the production classes. The
        // registered ones do not, and adding it to all three put both
        // build/classes and the module jar on the default test classpath --
        // harmless for JaCoCo, a mutant-loading hazard for PIT at M0.14.
        register<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project())
                implementation(testFixtures(project()))
            }
            targets.configureEach {
                testTask.configure {
                    // ⚠️ THE REPOSITORY ROOT, BECAUSE THE CONTAINER IS DESCRIBED
                    // THERE. A T3 case starts `docker-compose.test.yml`, which is
                    // the root's file and not the module's, and a test JVM's
                    // working directory is the module. Resolved at configuration
                    // time from the root project rather than walked for at
                    // runtime, so a case cannot go looking up the tree and find a
                    // sibling checkout's copy.
                    systemProperty("binjava.repoRoot", rootDir.absolutePath)
                }
            }
        }
        register<JvmTestSuite>("clusterTest") {
            dependencies {
                implementation(project())
                implementation(testFixtures(project()))
            }
        }
    }
}

// `check` runs the T0-T2 suite only, but it must still COMPILE the slower tiers:
// otherwise broken integration- or cluster-test code commits green and is not
// discovered until CI runs a tier nobody triggered.
tasks.named("check") {
    dependsOn("compileIntegrationTestJava", "compileClusterTestJava")
}

// testing.md rule 9: mutation score is the metric that measures whether tests
// constrain anything (M0.14). ADR-0045 records why this is jzap and not PIT.
//
// The plugin registers `mutationTest` (whole module), `mutationTestDiff`
// (changed lines since a base ref, defaulting to `HEAD..-Local-` -- staged and
// unstaged work, which is what a pre-commit gate wants for free) and, on the
// root project only, `mutationTestAll` (one pass across every module, so a
// test in one module can kill a mutant in another). `check-mutants.sh` is
// still `todo` (M0.14): it will drive `mutationTestDiff` rather than
// hand-rolling the `-PmutantTargets`/`--targetClasses` scoping the removed
// `pitest` task needed, because the plugin's diff mode replaces that
// machinery rather than sitting next to it.
jzap {
    // ⚠️ REQUIRED, NOT A DEFAULT OVERRIDE: the extension's own convention
    // falls back to `0.1.0-SNAPSHOT` when `project.version` is unset, which it
    // is here (no module in this build declares one) -- so leaving this unset
    // would resolve `io.github.huyz0:jzap-cli:0.1.0-SNAPSHOT`, a coordinate
    // that does not exist on Maven Central, rather than a real released
    // version. Verified against `JzapExtension.getEngineVersion()`'s javadoc
    // and `JzapPlugin.versionOf`. Kept level with the plugin coordinate in
    // buildSrc/build.gradle.kts -- the project publishes every module in
    // lockstep, so a mismatched pair is two different releases, not one.
    engineVersion = "0.1.1"

    // No `threshold` or `failOnSurvivors` here, matching the removed PIT
    // task's own restraint (it had no `--mutationThreshold` either): pass/fail
    // is `check-mutants.sh`'s decision to make from the report, not this
    // task's, so the same build.gradle.kts serves a future gate that wants to
    // read the JSON reporter and reason about it rather than trust an exit
    // code alone.
}
