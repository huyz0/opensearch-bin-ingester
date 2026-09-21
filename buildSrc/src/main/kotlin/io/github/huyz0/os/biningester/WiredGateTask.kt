// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.io.path.readLines

/** Checks M8's unwired-set table without invoking the historical Python scanner. */
abstract class WiredGateTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @TaskAction fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val spec = root.resolve("docs/internal/product/milestones/M8/SPEC.md")
        val backlog = root.resolve("docs/internal/product/backlog.md")
        val rows = table(spec)
        if (rows.isEmpty()) throw GradleException("wired gate: no unwired-set table in $spec")
        val open = openRows(backlog)
        val known = backlog.readText().lineSequence().filter { it.startsWith("| M") }
            .map { it.substringAfter('|').substringBefore('|').trim() }.toSet()
        val sources = Files.walk(root).use { stream -> stream.filter { it.toString().replace('\\', '/').contains("/src/main/") && it.fileName.toString().endsWith(".java") }.toList() }
            .map { path ->
                val normalized = path.toString().replace('\\', '/')
                val module = normalized.substringBefore("/src/main/").substringAfterLast('/')
                Triple(path, module, codeOnly(path.readText()))
            }
        val failures = mutableListOf<String>()
        rows.forEach { cells ->
            if (cells.size < 4) { failures += "${cells.firstOrNull() ?: "row"}: unreadable table row"; return@forEach }
            val predicates = predicates(cells[2])
            if (predicates.isEmpty()) { failures += "${cells[0]}: unreadable predicate cell"; return@forEach }
            val missing = predicates.filterNot { judge(it.first, it.second, sources) }
            if (missing.isNotEmpty()) {
                val owners = Regex("\\bM\\d+\\.\\d+[a-z]?\\b").findAll(cells[3]).map { it.value }.filter { it in open }.toList()
                if (owners.isEmpty()) {
                    val named = Regex("\\bM\\d+\\.\\d+[a-z]?\\b").findAll(cells[3]).map { it.value }.toList()
                    failures += "${cells[0]}: unwired; missing ${missing.joinToString { it.first + " " + it.second }}; " +
                        if (named.isEmpty()) "no open backlog owner" else "owner is done or missing"
                }
            }
        }
        if (failures.isNotEmpty()) throw GradleException("wired gate failed:\n" + failures.joinToString("\n"))
        logger.lifecycle("wired gate passed (${rows.size} unwired-set rows)")
    }

    private fun table(file: Path): List<List<String>> {
        var inside = false
        return file.readLines().mapNotNull { line ->
            if (line.startsWith("## ")) inside = line.trim() == "## The unwired set"
            if (inside && line.startsWith("| M")) line.trim().trim('|').split('|').map(String::trim) else null
        }
    }
    private fun openRows(file: Path): Set<String> = file.readLines().filter { it.startsWith("| M") }.mapNotNull {
        val cells = it.substring(1).split('|').map(String::trim)
        if (cells.size > 2 && !cells.last().startsWith("done")) cells.first() else null
    }.toSet()
    private fun codeOnly(text: String): String = text.replace(Regex("(?s)/\\*.*?\\*/"), " ").replace(Regex("(?m)//[^\\r\\n]*"), " ").replace(Regex("\"(?:\\\\.|[^\"\\\\])*\""), " ")
    private fun predicates(cell: String): List<Pair<String, String>> = Regex("`([^`]+)`").findAll(cell).mapNotNull {
        val span = it.groupValues[1].trim()
        if (span == "main") "main" to "" else {
            val p = span.split(Regex("\\s+"), limit = 2)
            if (p.size == 2 && p[0] in setOf("new", "new-impl", "call", "implements", "returns", "constant")) p[0] to p[1] else null
        }
    }.toList()
    private fun declares(code: String, name: String) = Regex("\\b(class|interface|record|enum)\\s+${Regex.escape(name)}\\b").containsMatchIn(code)
    private fun constructs(sources: List<Triple<Path, String, String>>, name: String): Boolean {
        val pattern = Regex("\\bnew\\s+${Regex.escape(name)}\\s*[(<]")
        if (sources.any { pattern.containsMatchIn(it.third) && !declares(it.third, name) }) return true
        if (!sources.any { declares(it.third, name) && pattern.containsMatchIn(it.third) }) return false
        val call = Regex("\\b${Regex.escape(name)}\\s*\\.\\w+\\s*\\(")
        return sources.any { !declares(it.third, name) && call.containsMatchIn(it.third) }
    }
    private fun implementors(sources: List<Triple<Path, String, String>>, iface: String): Set<String> = sources.flatMap {
        Regex("\\b(?:class|record|enum)\\s+(\\w+)[^{;]*\\bimplements\\b[^{}]*${Regex.escape(iface)}\\b").findAll(it.third).map { m -> m.groupValues[1] }.toList()
    }.toSet()
    private fun judge(kind: String, arg: String, s: List<Triple<Path, String, String>>): Boolean = when (kind) {
        "new" -> constructs(s, arg)
        "new-impl" -> implementors(s, arg).any { constructs(s, it) }
        "call" -> s.any { Regex("(?:\\.|::)${Regex.escape(arg)}\\b").containsMatchIn(it.third) &&
            !Regex("\\b\\w[\\w<>\\[\\], ?]*\\s+${Regex.escape(arg)}\\s*\\([^)]*\\)\\s*[{;]").containsMatchIn(it.third) }
        "implements" -> implementors(s, arg).isNotEmpty()
        "main" -> s.any { Regex("\\bpublic\\s+static\\s+void\\s+main\\s*\\(").containsMatchIn(it.third) }
        "returns" -> s.any { Regex("(?:^|[\\s.])${Regex.escape(arg).replace("\\ ", "\\\\s*")}\\s+\\w+\\s*\\(").containsMatchIn(it.third) }
        "constant" -> {
            val p = arg.split(Regex("\\s+in\\s+"), limit = 2)
            p.size == 2 && s.any { Regex("\\b${Regex.escape(p[0])}\\s*=(?!=)").containsMatchIn(it.third) } &&
                s.any { it.second == p[1] && Regex("\\b${Regex.escape(p[0])}\\b").containsMatchIn(it.third) && !Regex("\\b${Regex.escape(p[0])}\\s*=").containsMatchIn(it.third) }
        }
        else -> false
    }
}
