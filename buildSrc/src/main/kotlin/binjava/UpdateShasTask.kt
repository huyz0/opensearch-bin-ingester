// SPDX-License-Identifier: Apache-2.0
package binjava

import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.security.MessageDigest

/**
 * Write the missing `.jar.sha1` pins. The sanctioned way to accept a new
 * dependency, so accepting one is a visible diff rather than a silent resolve.
 *
 * ⚠️ It writes only MISSING pins. It will not overwrite one that exists and
 * disagrees -- that case is a published artifact changing under a version we
 * already reviewed, and the right response is to find out why, not to re-pin.
 */
abstract class UpdateShasTask : DefaultTask() {

    @get:Classpath
    abstract val dependencies: ConfigurableFileCollection

    @get:org.gradle.api.tasks.Internal
    abstract val licensesDir: DirectoryProperty

    @TaskAction
    fun update() {
        val dir = licensesDir.get().asFile.apply { mkdirs() }
        var written = 0
        for (jar in dependencies.files.filter { it.name.endsWith(".jar") }) {
            val sha = File(dir, jar.name + ".sha1")
            if (sha.exists()) continue
            sha.writeText(
                MessageDigest.getInstance("SHA-1").digest(jar.readBytes())
                    .joinToString("") { "%02x".format(it) } + "\n"
            )
            logger.lifecycle("pinned ${sha.name}")
            written++
        }
        if (written == 0) logger.lifecycle("updateShas: nothing missing")
        else logger.lifecycle("updateShas: wrote $written pin(s) -- review them, then add any missing <prefix>-LICENSE.txt")
    }
}
