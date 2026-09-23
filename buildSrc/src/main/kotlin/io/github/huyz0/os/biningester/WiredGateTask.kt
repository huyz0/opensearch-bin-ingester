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
    private fun predicates(cell: String): List<Pair<String, String>> {
        val spans = Regex("`([^`]+)`").findAll(cell).map { it.groupValues[1].trim() }.toList()
        val accepted = mutableListOf<Pair<String, String>>()
        for (span in spans) {
            if (span == "main") {
                accepted += "main" to ""
                continue
            }
            val p = span.split(Regex("\\s+"), limit = 2)
            if (p.size != 2 || p[0] !in setOf("new", "new-impl", "call", "implements", "returns", "constant") ||
                (p[0] == "call" && !Regex("[A-Za-z_$][\\w$]*(?:\\.[A-Za-z_$][\\w$]*){2,}").matches(p[1]))) {
                return emptyList()
            }
            accepted += p[0] to p[1]
        }
        return accepted
    }
    private fun declares(code: String, name: String) = Regex("\\b(class|interface|record|enum)\\s+${Regex.escape(name)}\\b").containsMatchIn(code)
    private fun packageName(code: String) = Regex("(?m)^\\s*package\\s+([\\w.]+)\\s*;")
        .find(code)?.groupValues?.get(1).orEmpty()
    private fun declaredTypes(sources: List<Triple<Path, String, String>>): Set<String> = sources.flatMap { source ->
        val pkg = packageName(source.third)
        Regex("\\b(?:class|interface|record|enum)\\s+([A-Za-z_$][\\w$]*)")
            .findAll(source.third).map { match -> if (pkg.isEmpty()) match.groupValues[1] else "$pkg.${match.groupValues[1]}" }
    }.toSet()
    private fun qualifiedType(name: String, code: String, knownTypes: Set<String>): String {
        if ('.' in name) return name
        val imported = Regex("\\bimport\\s+([\\w.]+)\\s*;").findAll(code)
            .map { it.groupValues[1] }.firstOrNull { it.substringAfterLast('.') == name }
        if (imported != null) return imported
        val pkg = packageName(code)
        val samePackage = if (pkg.isEmpty()) name else "$pkg.$name"
        if (samePackage in knownTypes) return samePackage
        val wildcardImports = Regex("\\bimport\\s+([\\w.]+)\\.\\*\\s*;")
            .findAll(code).map { it.groupValues[1] }.toSet()
        val candidates = knownTypes.filter { it.substringAfterLast('.') == name && it.substringBeforeLast('.', "") in wildcardImports }
        if (candidates.size == 1) return candidates.single()
        if (candidates.size > 1) return "<ambiguous>.$name"
        return samePackage
    }
    private fun methodScope(code: String, offset: Int): IntRange? {
        val controls = setOf("if", "for", "while", "switch", "catch", "synchronized", "try", "when")
        return Regex("\\b([A-Za-z_$][\\w$]*)\\s*\\([^{};]*\\)\\s*(?:throws\\s+[^{}]+)?\\{").findAll(code)
            .mapNotNull { match ->
                if (match.groupValues[1] in controls) return@mapNotNull null
                val open = match.range.last
                var depth = 0
                var close = -1
                for (index in open until code.length) {
                    if (code[index] == '{') depth++
                    if (code[index] == '}' && --depth == 0) { close = index; break }
                }
                if (close > open && offset in (open + 1 until close)) open + 1 until close else null
            }.minByOrNull { it.last - it.first }
    }
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
        "call" -> {
            val target = arg.split('.')
            if (target.size < 3) false else {
                val knownTypes = declaredTypes(s)
                val method = target.last()
                val type = target.dropLast(1).joinToString(".")
                val simpleType = type.substringAfterLast('.')
                val targetTypes = s.filter { declares(it.third, simpleType) }
                    .map { qualifiedType(simpleType, it.third, knownTypes) }.toSet()
                val targetType = type
                if (targetType !in targetTypes) false else {
                    val staticCall = Regex("\\b((?:[A-Za-z_$][\\w$]*\\.)*[A-Za-z_$][\\w$]*)\\s*(?:\\.|::)\\s*${Regex.escape(method)}\\s*\\(")
                    val methodCall = Regex("\\b(\\w+)\\s*(?:\\.|::)\\s*${Regex.escape(method)}\\s*\\(")
                    s.any { source ->
                        if (declares(source.third, simpleType) && qualifiedType(simpleType, source.third, knownTypes) == targetType) false else {
                            val declarations = Regex("\\b((?:[A-Za-z_$][\\w$]*\\.)*[A-Za-z_$][\\w$]*)\\s+(\\w+)\\b(?!\\s*\\()")
                                .findAll(source.third).map { match ->
                                    Triple(match.range.first, match.groupValues[1], match.groupValues[2])
                                }.toList()
                            staticCall.findAll(source.third)
                                .any { qualifiedType(it.groupValues[1], source.third, knownTypes) == targetType } ||
                                methodCall.findAll(source.third).any { call ->
                                    val callScope = methodScope(source.third, call.range.first)
                                    val visible = declarations.filter { declaration ->
                                        declaration.third == call.groupValues[1] &&
                                            (methodScope(source.third, declaration.first) == null ||
                                                methodScope(source.third, declaration.first) == callScope)
                                    }
                                    visible.isNotEmpty() && visible.all {
                                        qualifiedType(it.second, source.third, knownTypes) == targetType
                                    }
                                }
                        }
                    }
                }
            }
        }
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
