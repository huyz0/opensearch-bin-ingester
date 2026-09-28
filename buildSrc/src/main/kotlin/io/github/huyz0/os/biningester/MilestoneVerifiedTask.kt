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
        if (!Files.isRegularFile(verified)) throw GradleException("missing $verified")
        val specText = Files.readString(spec)
        val failures = try {
            MilestoneEvidence.unevidenced(specText, Files.readString(verified))
        } catch (malformed: IllegalArgumentException) {
            throw GradleException("$spec: ${malformed.message}")
        }
        if (failures.isNotEmpty()) throw GradleException("criteria without evidence: ${failures.joinToString()}")
        val criteria = MilestoneEvidence.criteriaCount(specText)
        logger.lifecycle("milestone evidence covers $criteria criteria")
    }
}
