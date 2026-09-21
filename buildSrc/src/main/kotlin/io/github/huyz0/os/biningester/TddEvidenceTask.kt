// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import groovy.json.JsonSlurper
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.nio.charset.StandardCharsets

/** Checks that newly added test methods have a byte-bound red-run record. */
abstract class TddEvidenceTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @TaskAction
    fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val renameMap = git(root, listOf("diff", "--cached", "--name-status", "-M" )).lineSequence()
            .map { it.split('\t') }.filter { it.size == 3 && it[0].startsWith("R") }
            .associate { it[2] to it[1] }
        val paths = git(root, listOf("diff", "--cached", "--name-only", "--diff-filter=ACMR")).trim().lines()
            .filter { it.contains("/src/") && it.endsWith(".java") && !it.contains("/src/main/") }
        val newTests = paths.filter { path ->
            val before = git(root, listOf("show", "HEAD:${renameMap[path] ?: path}"))
            val after = git(root, listOf("show", ":$path"))
            val afterIds = parseTestIds(after, path)
            val beforeIds = parseTestIds(before, renameMap[path] ?: path)
            val stableBefore = beforeIds.map { it.substringAfterLast('.', it) }.toSet()
            afterIds.any { it.substringAfterLast('.', it) !in stableBefore }
        }
        if (newTests.isEmpty()) {
            logger.lifecycle("TDD evidence: no newly added test methods")
            return
        }
        val record = root.resolve(".harness/tdd/red.json")
        if (!Files.isRegularFile(record)) throw GradleException("${newTests.size} test source(s) gained tests but .harness/tdd/red.json is missing")
        val text = Files.readString(record)
        val required = paths.flatMap { path ->
            val before = git(root, listOf("show", "HEAD:${renameMap[path] ?: path}"))
            val after = git(root, listOf("show", ":$path"))
            val old = parseTestIds(before, renameMap[path] ?: path).map { it.substringAfterLast('.', it) }.toSet()
            parseTestIds(after, path).filter { it.substringAfterLast('.', it) !in old }
                .map { it to sha256(after.toByteArray(StandardCharsets.UTF_8)) }
        }
        required.forEach { (id, sha) ->
            if (!redRecordMatches(text, id, sha)) throw GradleException("TDD red record is missing, non-red, or stale for $id")
        }
        logger.lifecycle("TDD evidence present for ${required.size} newly added test(s)")
    }

    companion object {
        fun testIdsFor(source: String, path: String): Set<String> = parseTestIds(source, path)

        private fun parseTestIds(source: String, path: String): Set<String> {
            val clean = source.replace(Regex("\"\"\"(?:[^\"\\\\]|\\\\.|\"(?!\"\")|\"\"(?!\"))*\"\"\"|(?s)/\\*.*?\\*/|//[^\\n]*|\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'")) { " ".repeat(it.value.length) }
        val pkg = Regex("(?m)^\\s*package\\s+([\\w.]+)\\s*;").find(clean)?.groupValues?.get(1)?.plus(".").orEmpty()
        val classes = Regex("\\b(?:class|interface|enum|record)\\s+([A-Za-z_$][\\w$]*)\\s*\\{").findAll(clean).map { match ->
            val open = match.range.last
            var depth = 0; var close = source.length - 1
            for (i in open until clean.length) { if (clean[i] == '{') depth++; if (clean[i] == '}') { depth--; if (depth == 0) { close = i; break } } }
            Triple(match.groupValues[1], match.range.first, close)
        }.toList()
        val pattern = Regex("(?s)@(?:[A-Za-z_$][\\w$]*\\.)*(?:Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate)\\b.*?\\b[A-Za-z_$][\\w$<>\\[\\], ?]*\\s+([A-Za-z_$][\\w$]*)\\s*\\(")
        return pattern.findAll(clean).map { match ->
            val chain = classes.filter { match.range.first in it.second..it.third }.map { it.first }.joinToString("$").ifEmpty { path.substringAfterLast('/').removeSuffix(".java") }
            "$pkg$chain#${match.groupValues[1]}"
        }.toSet()
        }
        fun redRecordMatches(text: String, id: String, sha: String): Boolean {
            val root = runCatching { JsonSlurper().parseText(text) as? Map<*, *> }.getOrNull() ?: return false
            val red = root["red"] as? Map<*, *> ?: return false
            val entry = red[id] as? Map<*, *> ?: return false
            return entry["sha256"] == sha && entry["at"] != null && entry["source"] is String
        }
    }
    private fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    private fun git(root: Path, args: List<String>): String {
        val p = ProcessBuilder(listOf("git") + args).directory(root.toFile()).redirectErrorStream(true).start()
        val out = p.inputStream.readBytes().toString(Charsets.UTF_8)
        if (p.waitFor() != 0) return ""
        return out
    }
}
