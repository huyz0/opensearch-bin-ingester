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
// constrain anything (M0.14).
//
// ⚠️ THE COMMAND-LINE ARTIFACT, NOT `gradle-pitest-plugin`. That plugin targets
// Gradle 8; this build is Gradle 9.7, and a plugin incompatibility would take
// the whole gate with it. Driving PIT as a plain JavaExec also keeps the
// classpath explicit, which is what lets the gate scope mutation to the classes
// a diff actually changed.
val pitest: Configuration by configurations.creating
// ⚠️ 1.21.x IS A FLOOR, NOT A PREFERENCE. `pitest-entry` SHADES ASM -- 149
// bundled classes -- so the ASM version is fixed by the PIT release and cannot
// be forced from outside; measured, forcing `org.ow2.asm:asm:9.10.1` onto the
// classpath changed nothing. 1.19.1's copy cannot read Java 25 bytecode:
// `IllegalArgumentException: Unsupported class file major version 69`, thrown
// before a single mutant is generated.
dependencies {
    pitest("org.pitest:pitest-command-line:1.21.1")
    pitest("org.pitest:pitest-junit5-plugin:1.2.3")
}

tasks.register<JavaExec>("pitest") {
    group = "verification"
    description = "PIT mutation coverage; -PmutantTargets scopes it to changed classes"
    dependsOn(tasks.named("testClasses"))
    mainClass.set("org.pitest.mutationtest.commandline.MutationCoverageReport")
    classpath = pitest + sourceSets.main.get().output + sourceSets.test.get().output +
        configurations.testRuntimeClasspath.get()

    val reportDir = layout.buildDirectory.dir("reports/pitest").get().asFile
    // ⚠️ Resolved at CONFIGURATION time, like `scratch` above: reading these
    // from a task action captures the Project and breaks the configuration
    // cache.
    val mainClassesDirs = sourceSets.main.get().output.classesDirs.asPath
    val sourceDirs = sourceSets.main.get().java.srcDirs.joinToString(",")
    val testClassesDirs = sourceSets.test.get().output.classesDirs.asPath
    val targets = (project.findProperty("mutantTargets") as String?)?.takeIf { it.isNotBlank() }

    // ⚠️ A module with no targets must NOT be silently skipped into a green
    // result -- that is the "reports success while measuring nothing" shape
    // build.md warns about. The gate script decides what to skip; this task
    // fails loudly if asked to mutate nothing.
    onlyIf { targets != null }

    doFirst {
        args = listOf(
            "--reportDir", reportDir.absolutePath,
            "--targetClasses", targets ?: "",
            "--targetTests", "binjava.*",
            "--sourceDirs", sourceDirs,
            "--classPath", "$mainClassesDirs:$testClassesDirs",
            "--outputFormats", "XML",
            "--timestampedReports", "false",
            "--testPlugin", "junit5",
            "--threads", Runtime.getRuntime().availableProcessors().toString(),
            "--failWhenNoMutations", "false",
        )
    }
}

tasks.register("pitestClasspath") {
    val cp = tasks.named<JavaExec>("pitest").map { it.classpath.files.map { f -> f.name } }
    doLast { cp.get().filter { it.contains("asm") }.forEach { println("CP: " + it) } }
}
