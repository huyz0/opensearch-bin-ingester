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
    // ⚠️ TEST ONLY, for `TestSequencers`. The reason is plain and NOT about
    // counting requests: since M4.6d a `DefaultIngest` REQUIRES a Sequencer, and
    // the tests here construct real ones over a real store. ⚠️ An earlier draft
    // of this comment claimed these tests count a commit-log append among the
    // PUTs they measure -- they do not, and no test under http/src/test reads
    // `counts()` at all. See TestSequencers' javadoc.
    testImplementation(testFixtures(project(":sequencer")))
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
//
// ⚠️ M3.5 adds MemoryFlatAtIntervalCeilingTest as the SAME tier, excluded and
// wired the same way: the reasoning above applies unchanged to a sibling
// proving NFR-6 at the adaptive interval's ceiling instead of at M1's fixed
// operating point.
tasks.named<Test>("test") {
    exclude("**/MemoryFlatUnderTenXBodySizeTest.class")
    exclude("**/MemoryFlatAtIntervalCeilingTest.class")
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

// ⚠️ M3.5; AC5 as amended by ADR-0026: the SAME 256 MB / no-OOM proof, once
// the interval has grown to its 5 s ceiling instead of sitting at the 250 ms
// floor, and under 24 CONCURRENT producers rather than M1.18's single one.
//
// ⚠️ A SEPARATE task, not bundled into `memoryBoundTest` above, because the
// two have genuinely different real costs and one shared budget would let the
// slower silently ride on the faster's headroom. Measured (ADR-0026): a lone
// producer at the ceiling moves ~44 KB/s, because `BulkService` appends in
// blocking 1000-record chunks and so advances one chunk per flush -- which is
// why the first attempt at this test, a single-producer copy of M1.18, timed
// out twice (at 8 and 20 minutes) still short of 200 MB. The concurrent shape
// this task now runs restores ~1 MiB/s aggregate.
val memoryBoundCeilingTest = tasks.register<Test>("memoryBoundCeilingTest") {
    group = "verification"
    description = "Criterion 8 (SPEC T12) at the interval's ceiling (M3.5): 200 MB body, 256 MB heap, no OOM."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("binjava.http.MemoryFlatAtIntervalCeilingTest")
        isFailOnNoMatchingTests = true
    }
    maxHeapSize = "256m"
    jvmArgs("-XX:+HeapDumpOnOutOfMemoryError",
            "-XX:HeapDumpPath=" + layout.buildDirectory.dir("tmp").get().asFile.absolutePath)
    // ⚠️ testing.md rule 17: scratch under build/tmp, never the system temp
    // directory. The java-conventions plugin sets this for every registered
    // JvmTestSuite target, but this is a standalone `tasks.register<Test>`
    // (like `memoryBoundTest`) and so inherits none of it -- measured: this
    // test streams 200 MB through LocalFsBinStore and left ~175 MB per run in
    // /tmp, never cleaned. Set explicitly here, for the same reason the
    // convention sets it there.
    systemProperty("java.io.tmpdir",
            layout.buildDirectory.dir("tmp").get().asFile.absolutePath)
    // ⚠️ 20 minutes against the test's own 900s JUnit @Timeout, so the JUnit
    // timeout fires first and names the test rather than the task. ADR-0026
    // estimates ~192s of streaming; the rest is margin for the warm-up, GC
    // variance under a deliberately tight heap, and a slower machine.
    timeout.set(Duration.ofMinutes(20))
    extensions.configure<JacocoTaskExtension> { isEnabled = false }
}
