// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.nio.file.Files
import java.nio.file.Path

/** Ensures every numbered acceptance criterion has an evidence line. */
abstract class MilestoneVerifiedTask : DefaultTask() {
    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val milestone: DirectoryProperty

    @TaskAction
    fun verify() {
        val dir = milestone.get().asFile.toPath()
        val spec = dir.resolve("SPEC.md")
        val verified = dir.resolve("VERIFIED.md")
        if (!Files.isRegularFile(spec)) throw GradleException("missing $spec")
        val specText = Files.readString(spec)
        val section = Regex("(?ms)^## Acceptance criteria\\s*(.*?)(?=^## )").find(specText)?.groupValues?.get(1)
            ?: throw GradleException("$spec has no Acceptance criteria section")
        val criteria = Regex("(?m)^\\s*(-?\\d+)\\.\\s+(.+)$").findAll(section).toList()
        if (criteria.isEmpty()) throw GradleException("$spec has no numbered acceptance criteria")
        if (!Files.isRegularFile(verified)) throw GradleException("missing $verified")
        val evidence = Files.readString(verified)
        val failures = criteria.mapNotNull { match ->
            val number = match.groupValues[1]
            val line = Regex("(?m)^\\s*${Regex.escape(number)}\\.\\s+(.+)$").find(evidence)?.groupValues?.get(1)
            if (line == null || !Regex("(#|\\.sh|\\bgradlew\\b|Test\\b|NOT-RUN|OBSERVED-NOT)").containsMatchIn(line)) number else null
        }
        if (failures.isNotEmpty()) throw GradleException("criteria without evidence: ${failures.joinToString()}")
        logger.lifecycle("milestone evidence covers ${criteria.size} criteria")
    }
}
