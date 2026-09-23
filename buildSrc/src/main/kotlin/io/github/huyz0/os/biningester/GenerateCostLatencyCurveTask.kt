// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

abstract class GenerateCostLatencyCurveTask : DefaultTask() {

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val resultsDirectory: DirectoryProperty

    @get:OutputFile
    abstract val outputFile: RegularFileProperty

    @get:OutputFile
    abstract val svgFile: RegularFileProperty

    @TaskAction
    fun generate() {
        CostLatencyCurveGenerator.main(
            arrayOf("--write", resultsDirectory.get().asFile.absolutePath,
                outputFile.get().asFile.absolutePath, svgFile.get().asFile.absolutePath),
        )
    }
}
