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

/** Refuses removal of test assertions without an explicit commit-body reason. */
abstract class TestIntegrityTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @get:Input
    @get:Optional
    abstract val commitMessageFile: Property<String>

    @TaskAction
    fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val statuses = ProcessBuilder("git", "diff", "--cached", "--name-status", "-M").directory(root.toFile()).redirectErrorStream(true).start()
            .inputStream.readBytes().toString(Charsets.UTF_8)
        if (statuses.lineSequence().none { fields ->
                val parts = fields.split('\t')
                parts.isNotEmpty() && !parts[0].startsWith("R") &&
                    parts.drop(1).any { it.contains("/src/") && !it.contains("/src/main/") && it.endsWith(".java") }
            }) return
        val testPaths = statuses.lineSequence().map { it.split('\t') }.filter { parts ->
            parts.isNotEmpty() && !parts[0].startsWith("R") && parts.drop(1).any { it.contains("/src/") && !it.contains("/src/main/") && it.endsWith(".java") }
        }.flatMap { it.drop(1).asSequence() }.toList()
        val removedTest = testPaths.any { path ->
            val process = ProcessBuilder("git", "diff", "--cached", "-U0", "--", path).directory(root.toFile()).redirectErrorStream(true).start()
            val diff = process.inputStream.readBytes().toString(Charsets.UTF_8)
            process.waitFor()
            diff.lineSequence().any { it.startsWith("-") && !it.startsWith("---") && Regex("@Test|assert[A-Z]|assertThat").containsMatchIn(it) }
        }
        if (!removedTest) return
        val msg = commitMessageFile.orNull?.let { root.resolve(it).takeIf { path -> Files.isRegularFile(path) }?.let { path -> Files.readString(path) } }.orEmpty()
        if (!Regex("(?i)justify|intentional|remove(?:d)? test|replace(?:d)? test|obsolete").containsMatchIn(msg))
            throw GradleException("a staged test assertion changed without a commit-body justification")
    }
}
