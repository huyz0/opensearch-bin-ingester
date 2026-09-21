// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.nio.file.Files
import kotlin.io.path.readText

/** Checks review override round and finding claims using both verdict stores. */
abstract class OverrideGateTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty
    @TaskAction fun verify() {
        val root = repository.get().asFile.toPath()
        val file = root.resolve("review/overrides.md")
        if (!Files.isRegularFile(file)) return
        val changedLines = stagedLines(root, "review/overrides.md")
        if (changedLines.isEmpty()) { logger.lifecycle("override gate passed (no changed override entries)"); return }
        val text = file.readText()
        val blocks = text.split(Regex("\\n\\s*\\n")).filter { block ->
            Regex("^M\\d+\\.\\w+\\s+[-(]").containsMatchIn(block) && changedLines.any { line ->
                val start = text.take(text.indexOf(block)).count { it == '\n' } + 1
                line >= start && line <= start + block.count { it == '\n' }
            }
        }
        val failures = mutableListOf<String>()
        blocks.forEach { block ->
            val task = Regex("^(M\\d+\\.\\w+)").find(block.trim())?.groupValues?.get(1) ?: return@forEach
            val hashes = mutableSetOf<String>(); val findings = mutableSetOf<String>()
            listOf(root.resolve(".harness/review"), root.resolve("review/verdicts").resolve(task)).forEach { store ->
                if (Files.isDirectory(store)) Files.walk(store).use { paths -> paths.filter { it.toString().endsWith(".json") }.forEach { path ->
                    runCatching { JsonSlurper().parse(path.toFile()) as? Map<*, *> }.getOrNull()?.let { json ->
                        if (json["task"] == task) { (json["diff_sha256"] as? String)?.let(hashes::add); (json["findings"] as? List<*>)?.forEach { f -> (f as? Map<*, *>)?.get("id")?.toString()?.substringAfterLast('-')?.let(findings::add) } }
                    }
                } }
            }
            if (hashes.isEmpty()) return@forEach
            val count = Regex("\\b(ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN|\\d+) ROUNDS\\b", RegexOption.IGNORE_CASE).find(block)?.groupValues?.get(1)?.let { words[it.uppercase()] ?: it.toInt() }
            if (count != null && count > hashes.size) failures += "$task states $count rounds; ${hashes.size} recorded"
            Regex("\\b([PT]\\d{1,3})\\b").findAll(block).map { it.groupValues[1] }.distinct().filter { it !in findings }
                .forEach { failures += "$task cites finding $it with no verdict record" }
        }
        if (failures.isNotEmpty()) throw GradleException("override gate failed:\n" + failures.joinToString("\n"))
        logger.lifecycle("override gate passed")
    }
    private fun stagedLines(root: java.nio.file.Path, path: String): Set<Int> {
        val process = ProcessBuilder("git", "diff", "--cached", "-U0", "--", path).directory(root.toFile()).redirectErrorStream(true).start()
        val text = process.inputStream.bufferedReader().readText(); process.waitFor()
        val lines = mutableSetOf<Int>()
        Regex("^@@ -[0-9,]+ \\+([0-9]+)(?:,(\\d+))? @@", RegexOption.MULTILINE).findAll(text).forEach {
            val start = it.groupValues[1].toInt(); val count = it.groupValues[2].ifEmpty { "1" }.toInt()
            if (count == 0) lines += start
            else (start until start + count).forEach(lines::add)
        }
        return lines
    }
    private val words = mapOf("ONE" to 1, "TWO" to 2, "THREE" to 3, "FOUR" to 4, "FIVE" to 5, "SIX" to 6, "SEVEN" to 7, "EIGHT" to 8, "NINE" to 9, "TEN" to 10)
}
