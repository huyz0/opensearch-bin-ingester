// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import groovy.json.JsonSlurper

/** Verifies local review evidence against the exact staged Git diff bytes. */
abstract class ReviewEvidenceTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @TaskAction
    fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val paths = git(root, listOf("diff", "--cached", "--name-only", "--diff-filter=ACMRD"))
            .trim().lineSequence().filter { it.isNotBlank() }.toList()
        if (paths.isEmpty()) {
            logger.lifecycle("review evidence skipped: no staged changes")
            return
        }
        val diff = gitBytes(root, listOf("diff", "--cached"))
        val sha = sha256(diff)
        val roles = if (paths.any(::isTestEligible)) setOf("reviewer", "test-reviewer") else setOf("reviewer")
        val reviewDir = root.resolve(".harness/review")
        val failures = roles.filter { !Files.isRegularFile(reviewDir.resolve("$sha.$it.json")) }
            .map { "no '$it' verdict for staged diff $sha" }.toMutableList()
        if (failures.isNotEmpty()) {
            throw GradleException("review evidence gate failed:\n" + failures.joinToString("\n"))
        }
        val verdicts = roles.map { role -> JsonSlurper().parse(reviewDir.resolve("$sha.$role.json").toFile()) as? Map<*, *> }
        verdicts.forEachIndexed { index, verdict ->
            if (verdict == null || !isValidVerdict(verdict, sha))
                throw GradleException("invalid review verdict for ${roles.elementAt(index)}")
            val findings = verdict["findings"] as? List<*> ?: throw GradleException("review findings are not a list")
            findings.forEach { finding ->
                val map = finding as? Map<*, *> ?: throw GradleException("malformed review finding")
                if (map["id"] !is String || map["severity"] !in setOf("minor", "major", "blocking") || map["status"] !in setOf("open", "resolved") || map["failure_scenario"] !is String)
                    throw GradleException("malformed review finding")
                if (verdict["verdict"] == "pass" && map["severity"] in setOf("major", "blocking") && map["status"] == "open")
                    throw GradleException("pass verdict carries an open major finding")
            }
            findings.filterIsInstance<Map<*, *>>().filter { it["status"] == "open" && it["severity"] in setOf("major", "blocking") }
                .filter { !baselineResolves(root, roles.elementAt(index), it["id"]?.toString().orEmpty()) }
                .forEach { throw GradleException("unresolved ${it["severity"]} finding ${it["id"]} in ${roles.elementAt(index)} review") }
        }
        val task = (verdicts.first()!!["task"] as String)
        val hashes = Files.list(reviewDir).use { stream ->
            stream.filter { it.fileName.toString().endsWith(".json") }.toList()
                .mapNotNull { path -> runCatching { JsonSlurper().parse(path.toFile()) as? Map<*, *> }.getOrNull() }
                .filter { it["task"] == task }.mapNotNull { it["diff_sha256"]?.toString() }.toSet()
        }
        val budget = parseBudget(System.getenv("REVIEW_ROUND_BUDGET"))
        if (hashes.size > budget) throw GradleException("review round budget exceeded for $task")
        logger.lifecycle("staged diff $sha has ${roles.joinToString()} review evidence")
    }

    private fun baselineResolves(root: Path, role: String, id: String): Boolean {
        val baseline = root.resolve(".harness/review/baselines/review.txt")
        return Files.isRegularFile(baseline) && Files.readString(baseline).lineSequence().any { it.trim() == "$role:$id" }
    }

    companion object {
        fun parseBudget(value: String?): Int = value?.let { it.toIntOrNull() ?: throw GradleException("invalid REVIEW_ROUND_BUDGET") } ?: 3

        fun isValidVerdict(verdict: Map<*, *>, sha: String): Boolean =
            verdict["diff_sha256"] == sha && verdict["task"] is String &&
                verdict["verdict"] in setOf("pass", "changes-requested") &&
                (verdict["findings"] as? List<*>)?.all { finding ->
                    val map = finding as? Map<*, *> ?: return@all false
                    map["id"] is String && map["severity"] in setOf("minor", "major", "blocking") &&
                        map["status"] in setOf("open", "resolved") && map["failure_scenario"] is String &&
                        !(verdict["verdict"] == "pass" && map["status"] == "open" && map["severity"] in setOf("major", "blocking"))
                } == true
    }

    private fun isTestEligible(path: String): Boolean = path.contains("/src/") || path.contains("\\src\\") ||
        path.endsWith(".java") || path.endsWith(".kt") || path.endsWith(".gradle.kts") ||
        path.startsWith("scripts/") || path == ".pre-commit-config.yaml" || path.startsWith(".github/workflows/")

    private fun git(root: Path, args: List<String>): String = gitBytes(root, args).toString(Charsets.UTF_8)

    private fun gitBytes(root: Path, args: List<String>): ByteArray {
        val process = ProcessBuilder(listOf("git") + args).directory(root.toFile()).redirectErrorStream(false).start()
        val output = ByteArrayOutputStream()
        process.inputStream.use { it.copyTo(output) }
        val error = process.errorStream.bufferedReader().readText()
        val status = process.waitFor()
        if (status != 0) throw GradleException("git ${args.joinToString(" ")} failed: $error")
        return output.toByteArray()
    }

    private fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes)
        .joinToString("") { "%02x".format(it) }
}
