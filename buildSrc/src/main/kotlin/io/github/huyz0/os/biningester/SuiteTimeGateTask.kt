// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction

/** Enforces the documented wall-clock budget for a measured suite run. */
abstract class SuiteTimeGateTask : DefaultTask() {
    @get:Input
    abstract val layer: Property<String>
    @get:Input
    abstract val seconds: Property<Double>

    @TaskAction
    fun verify() {
        val budget = mapOf("L0" to 90.0, "L1" to 300.0, "L2" to 600.0, "L2S" to 600.0, "L3" to 900.0)[layer.get()]
            ?: throw GradleException("unknown suite layer ${layer.get()}")
        if (seconds.get() > budget) throw GradleException("${layer.get()} took ${seconds.get()}s; budget is ${budget}s")
        logger.lifecycle("${layer.get()} ${seconds.get()}s within ${budget}s")
    }
}
