// SPDX-License-Identifier: Apache-2.0

import io.github.huyz0.os.biningester.AwsSdkHttp
import io.github.huyz0.os.biningester.DependencyLicensesTask
import io.github.huyz0.os.biningester.UpdateShasTask
import io.github.huyz0.os.biningester.RepositoryGatesTask
import io.github.huyz0.os.biningester.MilestoneVerifiedTask
import io.github.huyz0.os.biningester.ReviewEvidenceTask
import io.github.huyz0.os.biningester.TddEvidenceTask
import io.github.huyz0.os.biningester.TestIntegrityTask
import io.github.huyz0.os.biningester.CoverageGateTask
import io.github.huyz0.os.biningester.SuiteTimeGateTask
import io.github.huyz0.os.biningester.WiredGateTask
import io.github.huyz0.os.biningester.OverrideGateTask

// `base` gives the root project the `check` and `build` lifecycle tasks. Without
// it, `tasks.register("check")` silently created a THIRD, unrelated task and
// `./gradlew build --rerun-tasks` completed green without ever running the
// licence gate -- a gate that is not wired is a preference.
plugins { base }

// Maven Central namespace for this GitHub-owned project. Artifact IDs remain
// module-specific; Java packages use the corresponding hyphen-free namespace.
group = "io.github.huyz0.os.bin-ingester"

// The root project holds no code. Modules apply `io.github.huyz0.os.biningester.java-conventions`
// from buildSrc; this file names the build and owns the licence gate.
description = "Bundled object-store ingestion for OpenSearch pull-based ingest"

// ---------------------------------------------------------------------------
// Dependency licences, modelled on OpenSearch's own precommit task.
//
// Every dependency jar has a pinned SHA-1 and a committed licence under
// licenses/. Both live in the tree, so accepting a dependency is a diff someone
// reads rather than a line in a report regenerated on every build.
//
// ⚠️ One configuration, at the root, built from the version catalogue. Gradle 9
// forbids resolving another project's configuration, and every module today
// takes its dependencies from the catalogue, so this is the complete set.
//
// ⚠️ THAT IS AN ASSUMPTION, NOT AN INVARIANT. A module declaring a literal
// coordinate would bypass this gate entirely, and a GPL-2.0 dependency added
// that way passes `check` today. Backlog M0.19 makes it a predicate; until it
// lands, this completeness claim rests on nothing but habit.
// ---------------------------------------------------------------------------
val licenseCheck = configurations.create("licenseCheck") {
    isCanBeConsumed = false
    isCanBeResolved = true
    // ⚠️ THE SAME EXCLUSIONS THE MODULE DECLARES, from the same list. Without
    // them this gate demands a pinned sha and a committed licence for Netty,
    // Apache HttpClient 5 and their transitives -- sixteen jars that no module
    // puts on any classpath. A gate that over-reports is not unsafe, but it
    // makes accepting a real dependency a diff nobody reads.
    AwsSdkHttp.EXCLUDED_CLIENTS.forEach { exclude(group = AwsSdkHttp.GROUP, module = it) }
}

val libs = extensions.getByType<VersionCatalogsExtension>().named("libs")
libs.libraryAliases.forEach { alias ->
    libs.findLibrary(alias).ifPresent { dependencies.add(licenseCheck.name, it) }
}

val licensesDirectory = layout.projectDirectory.dir("licenses")

// A family of artifacts shares one licence file: every junit-* is `junit`.
// ⚠️ opentest4j and apiguardian are Apache-2.0, NOT EPL-2.0 like the junit-*
// artifacts they ship alongside. Folding them under `junit` would attribute
// them to the wrong licence.
val licenceMappings = mapOf(
    // ⚠️ junit 4 IS NOT junit 5's LICENCE. `junit:junit:4.13.2` arrives
    // transitively through `org.opensearch.test:framework`, and its bare
    // artifactId does not match `^junit-.*`, so without this line it fell to the
    // `junit` prefix and was labelled EPL-2.0. Its POM declares Eclipse Public
    // License 1.0 -- a different licence, with a different Secondary Licenses
    // clause and a different patent-defence trigger. The identifier is what the
    // deny-list matches, so a wrong one is worse than a missing one.
    "^junit$" to "junit4",
    "^junit-.*" to "junit",
    "^opentest4j$" to "opentest4j",
    "^apiguardian-api$" to "apiguardian",
    "^assertj-.*" to "assertj",
    "^byte-buddy.*" to "byte-buddy",
    // ⚠️ One prefix for ~36 artifacts. Helidon ships the runtime as many small
    // jars (helidon-common-*, helidon-http-*, helidon-webserver-*) all under the
    // same Apache-2.0 licence from the same project, so a per-jar licence file
    // would be 36 identical copies -- and 36 places for one of them to drift.
    "^helidon.*" to "helidon",
    // ⚠️ THE AWS SDK v2 IS 29 ARTIFACTS UNDER ONE APACHE-2.0 LICENCE, from one
    // project, so they share one licence file the way Helidon's 36 do. ⚠️ BUT
    // THEY ARE ENUMERATED AND ANCHORED RATHER THAN MATCHED BY A PREFIX, because
    // their artifactIds are GENERIC -- `annotations`, `auth`, `utils`, `regions`,
    // `profiles`, `checksums`. A pattern loose enough to catch them all would
    // silently relabel the next dependency that happens to publish an artifact
    // called `annotations` as Amazon's, and the SPDX id is the claim this gate
    // matches on. `eventstream` is NOT here: it is a separate project
    // (software.amazon.eventstream) and carries its own entry.
    "^annotations$" to "awssdk",
    "^arns$" to "awssdk",
    "^auth$" to "awssdk",
    "^aws-core$" to "awssdk",
    "^aws-query-protocol$" to "awssdk",
    "^aws-xml-protocol$" to "awssdk",
    "^checksums$" to "awssdk",
    "^checksums-spi$" to "awssdk",
    "^crt-core$" to "awssdk",
    "^endpoints-spi$" to "awssdk",
    "^http-auth$" to "awssdk",
    "^http-auth-aws$" to "awssdk",
    "^http-auth-aws-eventstream$" to "awssdk",
    "^http-auth-spi$" to "awssdk",
    "^http-client-spi$" to "awssdk",
    "^identity-spi$" to "awssdk",
    "^json-utils$" to "awssdk",
    "^metrics-spi$" to "awssdk",
    "^profiles$" to "awssdk",
    "^protocol-core$" to "awssdk",
    "^regions$" to "awssdk",
    "^retries$" to "awssdk",
    "^retries-spi$" to "awssdk",
    "^s3$" to "awssdk",
    "^sdk-core$" to "awssdk",
    "^third-party-jackson-core$" to "awssdk",
    "^url-connection-client$" to "awssdk",
    "^utils$" to "awssdk",
    "^utils-lite$" to "awssdk",
)

tasks.register<DependencyLicensesTask>("dependencyLicenses") {
    group = "verification"
    description = "Every dependency jar has a pinned sha and a committed licence"
    dependencies.from(licenseCheck)
    licensesDir.set(licensesDirectory)
    mappings.set(licenceMappings)
    // build.md rule 2, as EXACT SPDX identifiers matched against licenses/SPDX.txt.
    // Enumerated rather than matched by substring: "GPL-2.0" as a substring also
    // hits LGPL-2.0, and LGPL is a licence this project has not ruled out.
    denied.set(setOf(
        "GPL-1.0-only", "GPL-1.0-or-later", "GPL-2.0-only", "GPL-2.0-or-later",
        "GPL-3.0-only", "GPL-3.0-or-later", "GPL-2.0", "GPL-3.0",
        "AGPL-1.0-only", "AGPL-1.0-or-later", "AGPL-3.0-only", "AGPL-3.0-or-later", "AGPL-3.0",
        "SSPL-1.0", "EUPL-1.0", "EUPL-1.1", "EUPL-1.2",
        "CDDL-1.0", "BUSL-1.1",
    ))
    stamp.set(layout.buildDirectory.file("dependency-licenses.stamp"))
}

tasks.register<UpdateShasTask>("updateShas") {
    group = "verification"
    description = "Write missing licenses/*.jar.sha1 pins for new dependencies"
    dependencies.from(licenseCheck)
    licensesDir.set(licensesDirectory)
}

// It runs with `check` -- and therefore with `build` -- not as a separate step
// someone has to remember. The old script-based gate needed a report generated
// first, so a fresh clone failed its first commit.
tasks.named("check") { dependsOn("dependencyLicenses", "gates") }

tasks.register<RepositoryGatesTask>("gates") {
    group = "verification"
    description = "Run all repository gates using only Gradle and the JDK"
    repository.set(layout.projectDirectory)
    dependsOn("dependencyLicenses")
}

tasks.register("checkHarnessTests") {
    group = "verification"
    description = "Run the JVM-native buildSrc gate tests"
    val wrapperName = if (org.gradle.internal.os.OperatingSystem.current().isWindows) "gradlew.bat" else "gradlew"
    val wrapperPath = layout.projectDirectory.file(wrapperName).asFile.absolutePath
    val rootPath = layout.projectDirectory.asFile.absolutePath
    doLast {
        val process = ProcessBuilder(wrapperPath, "-p", "buildSrc", "nativeGateTest", "--no-daemon")
            .directory(java.io.File(rootPath)).inheritIO().start()
        check(process.waitFor() == 0) { "native buildSrc gate tests failed" }
    }
}

tasks.register<WiredGateTask>("checkWired") {
    group = "verification"
    description = "Verify the milestone unwired-set predicates using the JVM"
    repository.set(layout.projectDirectory)
    dependsOn("dependencyLicenses")
}

tasks.register<OverrideGateTask>("checkOverride") {
    group = "verification"
    description = "Verify review override claims using recorded verdicts"
    repository.set(layout.projectDirectory)
    dependsOn("dependencyLicenses")
}

tasks.named("gates") { dependsOn("checkWired", "checkOverride", "checkHarnessTests") }

tasks.register<RepositoryGatesTask>("checkCommitMessage") {
    group = "verification"
    description = "Validate a commit message file with the JVM gate runner"
    repository.set(layout.projectDirectory)
    commitMessageFile.set(providers.gradleProperty("commitMessageFile").orElse(".git/COMMIT_EDITMSG"))
}

tasks.register<MilestoneVerifiedTask>("checkMilestoneVerified") {
    group = "verification"
    description = "Verify every milestone acceptance criterion has evidence"
    val milestonePath = project.findProperty("milestoneDir")?.toString() ?: "docs/internal/product/milestones/M9"
    milestone.set(layout.projectDirectory.dir(milestonePath))
}

tasks.register<ReviewEvidenceTask>("checkReviewed") {
    group = "verification"
    description = "Verify staged review verdicts are bound to the staged diff"
    repository.set(layout.projectDirectory)
}

tasks.register<TddEvidenceTask>("checkTdd") {
    group = "verification"
    description = "Verify newly added tests have byte-bound red evidence"
    repository.set(layout.projectDirectory)
}

tasks.register<TestIntegrityTask>("checkTestIntegrity") {
    group = "verification"
    description = "Require a commit-body reason when staged tests are weakened"
    repository.set(layout.projectDirectory)
    commitMessageFile.set(providers.gradleProperty("commitMessageFile").orElse(".git/COMMIT_EDITMSG"))
}

tasks.register<CoverageGateTask>("checkCoverage") {
    group = "verification"
    description = "Check regenerated JaCoCo reports against coverage floors"
    repository.set(layout.projectDirectory)
}

gradle.projectsEvaluated {
    tasks.named("checkCoverage") {
        subprojects.forEach { subproject ->
            if (subproject.tasks.names.contains("test")) dependsOn("${subproject.path}:test")
            if (subproject.tasks.names.contains("jacocoTestReport")) dependsOn("${subproject.path}:jacocoTestReport")
        }
    }
}

val checkMutants = tasks.register("checkMutants") {
    group = "verification"
    description = "Run the native Gradle mutation-diff tasks for every module that provides one"
}
gradle.projectsEvaluated {
    checkMutants.configure {
        subprojects.filter { it.tasks.names.contains("mutationTestDiff") }
            .forEach { dependsOn("${it.path}:mutationTestDiff") }
    }
}

tasks.register<SuiteTimeGateTask>("checkSuiteTime") {
    group = "verification"
    description = "Check a measured suite duration against its layer budget"
    layer.set(project.findProperty("suiteLayer")?.toString() ?: "L0")
    seconds.set(project.findProperty("suiteSeconds")?.toString()?.toDoubleOrNull() ?: 0.0)
}
