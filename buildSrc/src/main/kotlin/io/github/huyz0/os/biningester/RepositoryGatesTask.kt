// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern
import kotlin.io.path.extension
import kotlin.io.path.isRegularFile
import kotlin.io.path.readText

/** Repository gates implemented in the JVM so a checkout needs only Gradle and Java. */
abstract class RepositoryGatesTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @get:Input
    @get:Optional
    abstract val commitMessageFile: Property<String>

    @TaskAction
    fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val failures = mutableListOf<String>()
        val files = trackedTree(root).toList()
        files.forEach { file ->
            val relative = root.relativize(file).toString().replace('\\', '/')
            val staged = stagedBytes(root, relative)
            if (staged != null && !stagedBlobMatches(root, relative, Files.readAllBytes(file))) failures += "working tree differs from staged blob: $relative"
        }

        checkHeaders(root, files, failures)
        checkFileSizes(root, files, failures)
        checkMarkdownLinks(root, files, failures)
        checkAdrReferences(root, files, failures)
        checkTerminology(root, files, failures)
        checkBuildWiring(root, failures)
        checkGeneratedIndexes(root, failures)
        RepositoryGateChecks.gateScope(root, failures)
        RepositoryGateChecks.javadocCitations(root, files, failures)
        RepositoryGateChecks.portability(root, failures)
        RepositoryGateChecks.metricCardinality(root, files, failures)
        RepositoryGateChecks.ioSeam(root, files, failures)
        RepositoryGateChecks.faultStore(root, files, failures)
        RepositoryGateChecks.testBudget(root, failures)
        RepositoryGateChecks.moduleDrift(root, failures)
        RepositoryGateChecks.moduleDependencies(root, failures)
        checkCommitMessage(root, failures)

        if (failures.isNotEmpty()) {
            throw GradleException("repository gates failed:\n" + failures.joinToString("\n"))
        }
        logger.lifecycle("repository gates passed (JVM/Gradle; ${files.size} repository files scanned)")
    }

    private fun trackedTree(root: Path): Sequence<Path> {
        val process = ProcessBuilder("git", "ls-files", "--cached", "--others", "--exclude-standard")
            .directory(root.toFile()).redirectErrorStream(true).start()
        val output = process.inputStream.readBytes().toString(Charsets.UTF_8)
        if (process.waitFor() != 0) throw GradleException("git ls-files failed: $output")
        return output.lineSequence().filter { it.isNotBlank() }
            .map { root.resolve(it).normalize() }.filter { Files.isRegularFile(it) }
    }

    private fun stagedBytes(root: Path, path: String): ByteArray? {
        val process = ProcessBuilder("git", "show", ":$path").directory(root.toFile()).redirectErrorStream(false).start()
        val bytes = process.inputStream.readBytes()
        return if (process.waitFor() == 0) bytes else null
    }

    companion object {
        fun stagedBlobMatches(root: Path, path: String, working: ByteArray): Boolean {
            val process = ProcessBuilder("git", "show", ":$path").directory(root.toFile()).redirectErrorStream(false).start()
            val staged = process.inputStream.readBytes()
            return process.waitFor() == 0 && contentMatches(path, staged, working)
        }

        fun sameContentForTest(staged: ByteArray, working: ByteArray): Boolean {
            return contentMatches("fixture.txt", staged, working)
        }

        fun terminologyExempt(path: String): Boolean {
            val normalized = path.replace('\\', '/')
            return normalized.endsWith("docs/internal/standards/glossary.md") ||
                normalized.contains("scripts") || normalized.substringAfterLast('/') == "RepositoryGatesTask.kt"
        }

        fun contentMatches(path: String, staged: ByteArray, working: ByteArray): Boolean {
            val text = path.substringAfterLast('.').lowercase() in setOf("java", "kt", "kts", "gradle", "md", "sh", "py", "yaml", "yml", "json", "txt", "properties", "sha1", "toml", "csv", "svg") ||
                path.substringAfterLast('/').lowercase() in setOf(".gitignore", "license", "notice", "gradlew", "gradlew.bat")
            if (!text) return staged.contentEquals(working)
            fun normalize(bytes: ByteArray): ByteArray = String(bytes, Charsets.UTF_8).replace("\r\n", "\n").toByteArray(Charsets.UTF_8)
            return normalize(staged).contentEquals(normalize(working))
        }
    }

    private fun checkHeaders(root: Path, files: List<Path>, failures: MutableList<String>) {
        val source = files.filter { it.extension in setOf("java", "kt", "kts", "gradle", "sh", "py") }
        source.filter { !Files.readAllLines(it).take(8).any { line -> line.contains("SPDX-License-Identifier: Apache-2.0") } }
            .forEach { failures += "missing Apache-2.0 header: ${root.relativize(it)}" }
    }

    private fun checkFileSizes(root: Path, files: List<Path>, failures: MutableList<String>) {
        files.filter { it.extension in setOf("java", "kt", "kts", "gradle", "sh", "py") }.forEach { file ->
            val lines = Files.readAllLines(file).size
            if (lines > 700) failures += "${root.relativize(file)} has $lines lines (limit 700)"
        }
    }

    private fun checkMarkdownLinks(root: Path, files: List<Path>, failures: MutableList<String>) {
        val link = Pattern.compile("\\]\\(([^)#]+)(?:#[^)]*)?\\)")
        files.filter { it.extension == "md" }.forEach { file ->
            val matcher = link.matcher(file.readText())
            while (matcher.find()) {
                val target = matcher.group(1)
                if (target.startsWith("http") || target.startsWith("mailto:")) continue
                val resolved = file.parent.resolve(target).normalize()
                if (!Files.exists(resolved)) failures += "${root.relativize(file)} -> $target"
            }
        }
    }

    private fun checkAdrReferences(root: Path, files: List<Path>, failures: MutableList<String>) {
        val adrs = files.filter { it.toString().contains("/decisions/") || it.toString().contains("\\decisions\\") }
            .mapNotNull { Regex("^(\\d+)-").find(it.fileName.toString())?.groupValues?.get(1) }
            .map { "ADR-${it.padStart(4, '0')}" }.toSet()
        val ref = Regex("ADR-(\\d+)")
        files.filter { it.extension in setOf("md", "java", "kt", "kts") }.forEach { file ->
            ref.findAll(file.readText()).map { "ADR-${it.groupValues[1]}" }.filter { it !in adrs }
                .distinct().forEach { failures += "${root.relativize(file)} cites missing $it" }
        }
    }

    private fun checkTerminology(root: Path, files: List<Path>, failures: MutableList<String>) {
        val deprecated = listOf(
            "service pod" to "ingester node", "service node" to "ingester node",
            "the service" to "the ingester", "client library" to "consumer library",
            "service-side" to "ingester-side", "ingester pod" to "ingester node",
            "collector" to "one of the six roles"
        )
        files.filter { it.extension in setOf("java", "kt", "kts", "md") }.forEach { file ->
            if (terminologyExempt(file.toString())) return@forEach
            file.readText().lineSequence().forEachIndexed { line, text ->
                if (!text.trimStart().startsWith("//") && !text.trimStart().startsWith("<!--") &&
                    deprecated.any { Regex("\\b${Regex.escape(it.first)}\\b", RegexOption.IGNORE_CASE).containsMatchIn(text) })
                    failures += "deprecated terminology at ${root.relativize(file)}:${line + 1}"
            }
        }
    }

    private fun checkBuildWiring(root: Path, failures: MutableList<String>) {
        val hooks = root.resolve(".pre-commit-config.yaml").readText()
        if (Regex("scripts/|python|bash|sh ").containsMatchIn(hooks))
            failures += ".pre-commit-config.yaml still invokes a shell/Python gate"
        if (!hooks.contains("gradlew.bat gates")) failures += "pre-commit does not invoke Gradle gates"
    }

    private fun checkGeneratedIndexes(root: Path, failures: MutableList<String>) {
        listOf(
            root.resolve("AGENTS.md") to listOf("skills", "standards", "gates"),
            root.resolve(".agents/skills/README.md") to listOf("skills")
        ).forEach { (file, regions) ->
            val text = if (Files.isRegularFile(file)) file.readText() else ""
            regions.forEach { region ->
                if (!text.contains("<!-- index:$region:start -->") || !text.contains("<!-- index:$region:end -->"))
                    failures += "${root.relativize(file)} is missing generated index region $region"
            }
        }
        val agents = root.resolve("AGENTS.md").readText()
        val gates = agents.substringAfter("<!-- index:gates:start -->", "").substringBefore("<!-- index:gates:end -->", "")
        if (Regex("scripts/|python|bash|\\.sh").containsMatchIn(gates))
            failures += "AGENTS.md generated gates index still names a shell/Python entry point"
        listOf("gates", "checkHarnessTests", "checkWired", "checkOverride", "checkReviewed", "checkTdd", "checkTestIntegrity",
            "checkCommitMessage", "checkMilestoneVerified", "checkCoverage", "checkSuiteTime", "checkMutants")
            .filterNot { gates.contains(it) }
            .forEach { failures += "AGENTS.md generated gates index omits Gradle task $it" }
    }

    private fun checkCommitMessage(root: Path, failures: MutableList<String>) {
        val value = commitMessageFile.orNull ?: return
        val file = root.resolve(value).normalize()
        if (!file.startsWith(root) || !file.isRegularFile()) {
            failures += "commit message file is not inside the repository: $value"
            return
        }
        val subject = file.readText().lineSequence().firstOrNull().orEmpty()
        if (subject.startsWith("Merge ") || subject.startsWith("Revert ") || subject.startsWith("fixup!")) return
        val id = Regex("^M-?\\d+\\.\\d+").find(subject)?.value
        if (id == null || !root.resolve("docs/internal/product/backlog.md").readText().contains(id))
            failures += "commit subject must start with a task ID present in the backlog: $subject"
    }
}
