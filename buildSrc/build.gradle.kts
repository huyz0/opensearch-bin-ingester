// SPDX-License-Identifier: Apache-2.0

// Convention plugins live here rather than in `subprojects {}` blocks: cross-
// project configuration is what makes a build resist ever being split up.
plugins { `kotlin-dsl` }

repositories { mavenCentral() }

// buildSrc holds real logic now -- the licence gate and the Java-test parser the
// TDD gates depend on -- so it gets tests like anything else.
//
// ⚠️ Versions come from the SAME catalogue as the modules (see settings.gradle.kts
// in this directory), so these jars are already pinned and licensed by the root
// `dependencyLicenses` task. A literal coordinate here would be outside it.
val libs = extensions.getByType<VersionCatalogsExtension>().named("libs")

dependencies {
    testImplementation(platform(libs.findLibrary("junit-bom").get()))
    testImplementation(libs.findLibrary("junit-jupiter").get())
    testImplementation(libs.findLibrary("assertj-core").get())
    testRuntimeOnly(libs.findLibrary("junit-platform-launcher").get())
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    maxHeapSize = "512m"
    testLogging { events("failed") }

    // ⚠️ The suite EXECS `scripts/*.py`; Gradle cannot infer that, so without
    // these declarations it sees no change and reports UP-TO-DATE on the only
    // edit the suite exists to catch. Verified: mutate the parser after a green
    // run and `check-harness-tests.sh` printed "ok 8 harness test(s) pass" while
    // --rerun-tasks showed the case genuinely failing. A test that does not run
    // on the change it guards is the same defect as a gate that reports success
    // while checking nothing.
    inputs.files(fileTree(rootDir.parentFile.resolve("scripts")) { include("*.py", "*.sh") })
        .withPropertyName("harnessScripts")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir(layout.projectDirectory.dir("src/test/resources"))
        .withPropertyName("javaFixtures")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    // ⚠️ Same reasoning as harnessScripts, and missed on the first pass: three
    // suites assert against the module list derived from settings.gradle.kts,
    // so without this the task is UP-TO-DATE across exactly the change they
    // exist to catch -- a module rename.
    // ⚠️ FreshCheckoutTest's fixture runs `git init && git add -A`, and in a
    // freshly-init'd repo every file is untracked -- so `git add -A` honours the
    // copied .gitignore. A new pattern matching a tracked path makes the
    // fixture's tracked list differ from the source's and fails the
    // post-condition. Without this declaration the suite would go UP-TO-DATE
    // across exactly that edit.
    //
    // ⚠️ An earlier version of this comment credited a check-gate-scope test
    // that has since moved to M0.31, and carried a "Measured:" for it. The
    // declaration was right and its stated reason was not.
    inputs.file(rootDir.parentFile.resolve(".gitignore"))
        .withPropertyName("rootGitignore")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.file(rootDir.parentFile.resolve("settings.gradle.kts"))
        .withPropertyName("rootSettings")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

