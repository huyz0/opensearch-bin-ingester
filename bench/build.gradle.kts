// SPDX-License-Identifier: Apache-2.0

// The benchmark harness (ADR-0059). It depends on NOTHING in the production
// module graph today -- the load generator and results writer are benchmark
// tooling only -- and that boundary is the point: these dependencies reach no
// module that ships.
//
// ⚠️ NOTHING MAY DEPEND ON THIS MODULE. It is a leaf like `server`, for the
// opposite reason: `server` may name everything, `bench` may be named by
// nothing. check-module.sh enforces both leaves; ADR-0059 § Consequences says
// why that boundary matters.
plugins { id("io.github.huyz0.os.biningester.java-conventions") }

sourceSets {
    named("main") {
        java.srcDir("src/jmh/java")
        resources.srcDir("src/jmh/resources")
    }
}

dependencies {
    implementation(libs.hdrhistogram)
    implementation(libs.jmh.core)
    implementation(libs.awssdk.http.auth.aws)
    implementation(libs.zstd.jni)
    implementation(libs.lz4.java)
    annotationProcessor(libs.jmh.generator.annprocess)
}

val jmhResult = layout.buildDirectory.file("jmh.json")
val gateResult = layout.buildDirectory.file("allocation-gate.json")
val jmhInclude = providers.gradleProperty("jmhInclude").orElse("")
val jmh = tasks.register<JavaExec>("jmh") {
    group = "verification"
    description = "Run the full benchmark suite with the annotation-declared fork and warmup counts"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    val include = jmhInclude.get().trim()
    if (include.isNotEmpty()) args(include)
    providers.gradleProperty("jmhWarmups").orNull?.let { args("-wi", it) }
    providers.gradleProperty("jmhWarmupTime").orNull?.let { args("-w", it) }
    providers.gradleProperty("jmhIterations").orNull?.let { args("-i", it) }
    providers.gradleProperty("jmhMeasurementTime").orNull?.let { args("-r", it) }
    providers.gradleProperty("jmhForks").orNull?.let { args("-f", it) }
    args("-prof", "gc", "-rf", "json", "-rff", jmhResult.get().asFile.absolutePath)
}

val gateJmh = tasks.register<JavaExec>("allocationGateJmh") {
    group = "verification"
    description = "Run the named one-fork JMH allocation benchmark set"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    args(
        "-wi", "1", "-w", "1s", "-i", "2", "-r", "1s", "-f", "1",
        "-prof", "gc", "-rf", "json", "-rff", gateResult.get().asFile.absolutePath,
    )
}

tasks.register<Exec>("checkAllocationGate") {
    group = "verification"
    description = "Run the capped one-fork JMH allocation gate"
    dependsOn(gateJmh)
    commandLine(
        "python",
        rootProject.file("scripts/check-allocation-gate.py").absolutePath,
        "--baseline", project.file("src/jmh/resources/allocation-baseline.json").absolutePath,
        "--results", gateResult.get().asFile.absolutePath,
    )
}

tasks.named("check") { dependsOn("checkAllocationGate") }
