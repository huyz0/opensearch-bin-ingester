// SPDX-License-Identifier: Apache-2.0

import java.time.Duration

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":ingest"))
    // ⚠️ Helidon lives HERE and nowhere below. check-module.sh asserts no module
    // below `http` resolves an HTTP dependency (SPEC T6c) -- that classpath
    // constraint, not a test, is what keeps this an adapter rather than a
    // second implementation of the ingest path.
    implementation(libs.helidon.webserver)

    testImplementation(libs.helidon.webclient)
    // ⚠️ TEST ONLY. Criterion 1 is "202 only after durable", which a fake Ingest
    // cannot demonstrate -- it needs the real DefaultIngest over a real store.
    // Nothing here reaches the production classpath, and rule 4 still holds:
    // `http` depends on these, not the other way round.
    testImplementation(project(":binstore-backends"))
}

// ⚠️ T12 / criterion 8 (M1.18): a 200 MB `_bulk` body ingested under a 256 MB
// heap, no OOM. Reusing the `test` source set's compiled classes, not a whole
// registered JvmTestSuite -- the shared convention's `maxHeapSize = "512m"`
// would prove nothing about the 256 MB budget this test exists to check, and
// the point is one differently-configured TASK, not a fourth tier.
//
// ⚠️ EXCLUDED from `test` (below) AND from `check` (not listed there, same as
// `integrationTest`/`clusterTest`): a heavier tier is explicit, on demand plus
// CI, matching build.md's fast-default-loop philosophy.
tasks.named<Test>("test") {
    exclude("**/MemoryFlatUnderTenXBodySizeTest.class")
}

val memoryBoundTest = tasks.register<Test>("memoryBoundTest") {
    group = "verification"
    description = "Criterion 8 (SPEC T12): 200 MB _bulk body under a 256 MB heap, no OOM."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("binjava.http.MemoryFlatUnderTenXBodySizeTest")
        isFailOnNoMatchingTests = true
    }
    maxHeapSize = "256m"
    jvmArgs("-XX:+HeapDumpOnOutOfMemoryError",
            "-XX:HeapDumpPath=" + layout.buildDirectory.dir("tmp").get().asFile.absolutePath)
    // ⚠️ Measured at ~1m56s on one run, ~4m14s on another clean, isolated run
    // (review round 2) -- streaming 200 MB through a real HTTP/parse/
    // accumulate/flush/store round trip under a deliberately tight heap is
    // slower than an unconstrained run, and both the slowness AND its
    // variance are an expected cost of proving THIS property under GC
    // pressure, not a bug. 10 minutes leaves real headroom over the slower
    // observed time and over the test's own 480s JUnit @Timeout (raised from
    // 300s for the same reason -- see MemoryFlatUnderTenXBodySizeTest).
    timeout.set(Duration.ofMinutes(10))
    // ⚠️ JaCoCo's own instrumentation adds real heap overhead -- measured:
    // running this same test under the default `test` task (JaCoCo attached)
    // showed ~200 MB used BEFORE a single body byte was written, which is
    // Helidon-plus-JaCoCo class-loading and instrumentation cost, not this
    // test's subject. `clusterTest` disables JaCoCo for the identical reason
    // (a coverage agent's own footprint is not what a memory-budget floor
    // measures).
    extensions.configure<JacocoTaskExtension> { isEnabled = false }
}
