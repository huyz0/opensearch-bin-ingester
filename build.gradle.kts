// SPDX-License-Identifier: Apache-2.0

import binjava.DependencyLicensesTask
import binjava.UpdateShasTask

// `base` gives the root project the `check` and `build` lifecycle tasks. Without
// it, `tasks.register("check")` silently created a THIRD, unrelated task and
// `./gradlew build --rerun-tasks` completed green without ever running the
// licence gate -- a gate that is not wired is a preference.
plugins { base }

// The root project holds no code. Modules apply `binjava.java-conventions`
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
tasks.named("check") { dependsOn("dependencyLicenses") }
