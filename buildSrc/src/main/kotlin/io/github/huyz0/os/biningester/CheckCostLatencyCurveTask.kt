// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

abstract class CheckCostLatencyCurveTask : DefaultTask() {

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val resultsDirectory: DirectoryProperty

    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val document: RegularFileProperty

    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val svg: RegularFileProperty

    @TaskAction
    fun check() {
        CostLatencyCurveGenerator.main(
            arrayOf("--check", resultsDirectory.get().asFile.absolutePath,
                document.get().asFile.absolutePath, svg.get().asFile.absolutePath),
        )
    }
}
