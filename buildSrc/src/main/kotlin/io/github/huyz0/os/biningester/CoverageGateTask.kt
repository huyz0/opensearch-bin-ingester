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
import javax.xml.parsers.DocumentBuilderFactory

/** Reads regenerated JaCoCo XML and enforces the project coverage floors. */
abstract class CoverageGateTask : DefaultTask() {
    @get:Internal
    abstract val repository: DirectoryProperty

    @TaskAction
    fun verify() {
        val root = repository.get().asFile.toPath().toAbsolutePath().normalize()
        val modules = Files.list(root).use { it.filter(Files::isDirectory).toList() }
            .filter { it.fileName.toString() != "buildSrc" && Files.isDirectory(it.resolve("src")) }
        val measured = mutableListOf<String>()
        val failures = mutableListOf<String>()
        modules.forEach { module ->
            val classes = module.resolve("build/classes/java/main")
            if (!Files.exists(classes)) return@forEach
            val reports = Files.walk(module.resolve("build/reports/jacoco")).use { stream ->
                stream.filter { it.fileName.toString() == "jacocoTestReport.xml" }.toList()
            }
            val report = reports.firstOrNull()
            if (report == null) {
                failures += "${module.fileName}: compiled classes exist but JaCoCo report is missing"
                return@forEach
            }
            val counters = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(report.toFile()).getElementsByTagName("counter")
            var lineMissed = 0L; var lineCovered = 0L; var branchMissed = 0L; var branchCovered = 0L
            for (i in 0 until counters.length) {
                val node = counters.item(i)
                when (node.attributes.getNamedItem("type")?.nodeValue) {
                    "LINE" -> { lineMissed = node.attributes.getNamedItem("missed").nodeValue.toLong(); lineCovered = node.attributes.getNamedItem("covered").nodeValue.toLong() }
                    "BRANCH" -> { branchMissed = node.attributes.getNamedItem("missed").nodeValue.toLong(); branchCovered = node.attributes.getNamedItem("covered").nodeValue.toLong() }
                }
            }
            if (lineMissed + lineCovered == 0L) failures += "${module.fileName}: JaCoCo has no line counters"
            else if (lineCovered.toDouble() / (lineMissed + lineCovered) < .95) failures += "${module.fileName}: line coverage below 95%"
            if (branchMissed + branchCovered > 0 && branchCovered.toDouble() / (branchMissed + branchCovered) < .90) failures += "${module.fileName}: branch coverage below 90%"
            measured += module.fileName.toString()
        }
        if (measured.isEmpty() && failures.isEmpty()) throw GradleException("coverage not measured: no compiled module has a report")
        if (failures.isNotEmpty()) throw GradleException("coverage gate failed:\n" + failures.joinToString("\n"))
        logger.lifecycle("coverage passed for ${measured.joinToString()}")
    }
}
