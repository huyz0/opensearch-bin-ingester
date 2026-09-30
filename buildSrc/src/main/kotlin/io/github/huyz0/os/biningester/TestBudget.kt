// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText

/**
 * M13.9 (M12 harvest R7): build.md's memory budget as a file predicate. Every
 * limit the runtime enforces is read from where it is set -- the Gradle
 * daemon's heap from gradle.properties, a compile daemon and a test JVM PER
 * WORKER from the conventions plugin, and every compose service's
 * `mem_limit` -- and their sum is refused above the ceiling build.md states.
 *
 * ⚠️ It ports the retired `scripts/test_budget.py`, which the JVM gates did
 * not: until M13.9 nothing read the compose file or the ceiling, so a raised
 * `mem_limit` passed every gate (M12 milestone review E9).
 *
 * ⚠️ A MODULE's OWN TEST HEAP (M13.9 review P1): a build script may set one no
 * larger than the conventions plugin's, which the sum already counts. A larger
 * one is refused unless it is in [HEAP_EXCEPTIONS], pinned at its value, and
 * an exception the script no longer sets is refused too. The exceptions are
 * NOT summed: build.md says what they cost.
 *
 * ⚠️ WHAT IT DOES NOT READ: build.md's table rows for the OpenSearch
 * container and the gateway under test are set by no file in this tree, so
 * they are not summed, as they were not before.
 */
object TestBudget {

    const val BUILD_MD = "docs/internal/standards/build.md"
    const val CONVENTIONS =
        "buildSrc/src/main/kotlin/io.github.huyz0.os.biningester.java-conventions.gradle.kts"

    /**
     * Build scripts whose test heap is above the conventions plugin's, each at
     * the one value it may set. `plugin`: `clusterTest` runs an OpenSearch node
     * in the test JVM (T4, opt-in; build.md § Budget).
     */
    val HEAP_EXCEPTIONS: Map<String, Int> = mapOf("plugin/build.gradle.kts" to 2048)

    private val SIZE = Regex("(\\d+(?:\\.\\d+)?)\\s*([gGmMkK]?)[bB]?")
    private val CEILING = Regex("\\*\\*Ceiling: (\\d+) GiB\\*\\*")
    private val TOP_KEY = Regex("^[A-Za-z0-9_-]+:")
    private val KEY = Regex("^(\\s+)([A-Za-z0-9_-]+):\\s*(?:#.*)?$")
    private val MEM_LIMIT = Regex("^\\s*mem_limit:\\s*['\"]?([^'\"#\\s]+)")
    private val MEMORY = Regex("^\\s*memory:\\s*['\"]?([^'\"#\\s]+)")
    private val LIMITS = Regex("^(\\s*)limits:\\s*(?:#.*)?$")
    private val HEAP = Regex("maxHeapSize\\s*=\\s*\"([^\"]+)\"")

    /** A memory size as compose and the JVM write it (`1g`, `512m`, `1.5g`, `512mb`), in MiB; null if unreadable. */
    fun mib(text: String): Int? {
        val m = SIZE.matchEntire(text.trim()) ?: return null
        val scale = when (m.groupValues[2].lowercase()) {
            "g" -> 1024.0
            "m" -> 1.0
            "k" -> 1.0 / 1024
            else -> 1.0 / (1024 * 1024)
        }
        return Math.round(m.groupValues[1].toDouble() * scale).toInt()
    }

    /** The ceiling build.md states as `**Ceiling: <n> GiB**`, in MiB; null if it states none. */
    fun ceilingMib(buildMd: String): Int? = CEILING.find(buildMd)?.groupValues?.get(1)?.toInt()?.times(1024)

    /**
     * Each service under the top-level `services:` key, with the memory limit its
     * OWN block declares, or null: its `mem_limit`, else a `memory` under a
     * `limits:` key (a `reservations` memory is a request, not a cap -- M13.9
     * review P3). A key nested inside a service is not a service, and neither
     * is anything under another top-level key.
     */
    fun composeLimits(compose: String): Map<String, String?> {
        val blocks = LinkedHashMap<String, MutableList<String>>()
        var inServices = false
        var indent: Int? = null
        var current: MutableList<String>? = null
        compose.lines().forEach { line ->
            if (TOP_KEY.containsMatchIn(line)) {
                inServices = line.startsWith("services:")
                indent = null
                current = null
                return@forEach
            }
            if (!inServices) return@forEach
            val key = KEY.find(line)
            if (key != null && indent == null) indent = key.groupValues[1].length
            if (key != null && key.groupValues[1].length == indent) {
                current = mutableListOf<String>().also { blocks[key.groupValues[2]] = it }
            } else {
                current?.add(line)
            }
        }
        return blocks.mapValues { (_, body) -> limitIn(body) }
    }

    private fun limitIn(body: List<String>): String? {
        body.firstNotNullOfOrNull { MEM_LIMIT.find(it)?.groupValues?.get(1) }?.let { return it }
        var limitsIndent: Int? = null
        for (line in body) {
            val indent = line.length - line.trimStart().length
            if (limitsIndent != null && line.isNotBlank() && indent <= limitsIndent) limitsIndent = null
            LIMITS.find(line)?.let { limitsIndent = it.groupValues[1].length }
            if (limitsIndent != null && indent > limitsIndent!!) MEMORY.find(line)?.let { return it.groupValues[1] }
        }
        return null
    }

    /** The configured limits by label, in MiB; what cannot be read is added to [failures]. */
    fun parts(root: Path, failures: MutableList<String>): Map<String, Int> {
        val parts = LinkedHashMap<String, Int>()
        val props = root.resolve("gradle.properties").readText()
        val heap = Regex("(?m)^org\\.gradle\\.jvmargs=.*?-Xmx(\\S+)").find(props)?.groupValues?.get(1)
        if (heap == null || mib(heap) == null) failures += "gradle.properties has no Gradle heap limit"
        else parts["Gradle daemon heap"] = mib(heap)!!
        val workers = Regex("(?m)^org\\.gradle\\.workers\\.max=(\\d+)").find(props)?.groupValues?.get(1)?.toInt()
        if (workers == null) failures += "gradle.properties has no worker limit"
        val perWorker = workers ?: 1
        val conventions = root.resolve(CONVENTIONS)
        val build = if (Files.isRegularFile(conventions)) conventions.readText() else ""
        val compile = Regex("memoryMaximumSize\\s*=\\s*\"([^\"]+)\"").find(build)?.groupValues?.get(1)?.let(::mib)
        if (compile == null) failures += "conventions plugin has no compile-daemon heap limit"
        else parts["compile daemons ($perWorker x $compile MiB)"] = perWorker * compile
        // ⚠️ The LARGEST test heap: every test task is bounded by its own, and
        // any of them can be the one a worker runs.
        val tests = HEAP.findAll(build).map { mib(it.groupValues[1]) }.toList()
        val test = if (tests.isEmpty() || tests.any { it == null }) null else tests.maxOf { it!! }
        if (test == null) failures += "conventions plugin has no test heap limit"
        else parts["test JVMs ($perWorker x $test MiB)"] = perWorker * test
        if (!build.contains("timeout.set")) failures += "conventions plugin has no test timeout"
        if (test != null) moduleHeaps(root, test, failures)
        composeFiles(root).forEach { file ->
            val name = file.fileName.toString()
            composeLimits(file.readText()).forEach { (service, limit) ->
                val size = limit?.let(::mib)
                when {
                    limit == null -> failures += "$name: service '$service' declares no memory limit"
                    size == null -> failures += "$name: service '$service' has an unreadable memory limit '$limit'"
                    size == 0 -> failures += "$name: service '$service' has a memory limit of 0, which Docker reads as none"
                    else -> parts["$name/$service"] = size
                }
            }
        }
        return parts
    }

    /** [parts], and their sum against build.md's ceiling. */
    fun check(root: Path, failures: MutableList<String>) {
        if (!Files.isRegularFile(root.resolve("gradle.properties"))) return
        val total = parts(root, failures).values.sum()
        val buildMd = root.resolve(BUILD_MD)
        val ceiling = if (Files.isRegularFile(buildMd)) ceilingMib(buildMd.readText()) else null
        when {
            ceiling == null -> failures += "$BUILD_MD states no **Ceiling: <n> GiB**"
            total > ceiling -> failures +=
                "the configured memory limits sum to $total MiB, above build.md's $ceiling MiB ceiling"
        }
    }

    /** Every module's and buildSrc's own test heaps against the conventions' [test] heap and [HEAP_EXCEPTIONS]. */
    private fun moduleHeaps(root: Path, test: Int, failures: MutableList<String>) {
        val scripts = Files.list(root).use { stream ->
            stream.map { it.resolve("build.gradle.kts") }.filter { Files.isRegularFile(it) }.sorted().toList()
        }
        val found = mutableSetOf<String>()
        scripts.forEach { script ->
            val name = root.relativize(script).toString().replace('\\', '/')
            HEAP.findAll(script.readText()).map { it.groupValues[1] }.forEach { value ->
                val size = mib(value)
                when {
                    size == null -> failures += "$name sets an unreadable test heap '$value'"
                    size <= test -> {}
                    HEAP_EXCEPTIONS[name] == size -> found += name
                    else -> failures += "$name sets a test heap of $size MiB, above the conventions" +
                        " plugin's $test MiB, and is not a named exception at that value"
                }
            }
        }
        (HEAP_EXCEPTIONS.keys - found).forEach {
            failures += "$it is a named test-heap exception at ${HEAP_EXCEPTIONS[it]} MiB but no longer sets it"
        }
    }

    /** The compose files at the checkout's root: `docker-compose*.yml` / `.yaml`. */
    private fun composeFiles(root: Path): List<Path> = Files.list(root).use { stream ->
        stream.filter { Files.isRegularFile(it) && Regex("docker-compose.*\\.ya?ml").matches(it.fileName.toString()) }
            .sorted().toList()
    }
}
