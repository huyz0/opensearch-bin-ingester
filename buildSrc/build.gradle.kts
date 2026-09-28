// SPDX-License-Identifier: Apache-2.0

// Convention plugins live here rather than in `subprojects {}` blocks: cross-
// project configuration is what makes a build resist ever being split up.
plugins { `kotlin-dsl` }

repositories {
    mavenCentral()
    // ADR-0045: the jzap Gradle plugin. Not mirrored to Maven Central -- its
    // own release notes call the Central copy "engine modules only" -- so the
    // plugin implementation jar comes from the Plugin Portal's own repository,
    // which is where `id("io.github.huyz0.jzap")` below resolves it from.
    gradlePluginPortal()
}

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

    // ADR-0045: buildSrc's own plugin classpath, so io.github.huyz0.os.biningester.java-conventions
    // can `id("io.github.huyz0.jzap")` it -- a literal coordinate outside the
    // catalog above, the same precedent the removed `pitest` Configuration
    // set: that catalog pins what modules compile and test against, and this
    // is neither.
    // ⚠️ 0.1.1, NOT 0.1.0: 0.1.0's `contributeToAggregate` resolved a
    // sibling project's test classpath eagerly inside `afterEvaluate`, which
    // this repo's own module graph -- `binstore-backends` depending on
    // `binstore-spi`'s test fixtures -- throws against on every build, not
    // only a mutation one. Filed and fixed upstream same-day; verified on
    // this tree with a local build before the 0.1.1 release existed.
    implementation("io.github.huyz0:jzap-gradle:0.1.1")
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

tasks.register<Test>("nativeGateTest") {
    description = "Run only the JVM-native repository gate tests"
    group = "verification"
    useJUnitPlatform()
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    filter {
        includeTestsMatching("io.github.huyz0.os.biningester.GradleGateWiringTest")
        includeTestsMatching("io.github.huyz0.os.biningester.RepositoryGateChecksTest")
        includeTestsMatching("io.github.huyz0.os.biningester.CostLatencyCurveGeneratorTest")
        includeTestsMatching("io.github.huyz0.os.biningester.L1TestCountTest")
        includeTestsMatching("io.github.huyz0.os.biningester.MilestoneEvidenceTest")
        includeTestsMatching("io.github.huyz0.os.biningester.FileSizeCeilingTest")
    }
}

