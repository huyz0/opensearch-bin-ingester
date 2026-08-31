// SPDX-License-Identifier: Apache-2.0
package binjava

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.security.MessageDigest

/**
 * Every dependency jar has a pinned SHA-1 and a committed licence, both in the
 * tree and both reviewable in a diff.
 *
 * Modelled on OpenSearch's `DependencyLicensesTask`, deliberately. The version
 * this replaced scraped POM `<licenses>` over the network at build time and was
 * wrong in five ways at once: it emitted POM prose ("Eclipse Public License
 * v2.0") while the gate grepped SPDX ids, so the deny-list could essentially
 * never match; it never followed `<parent>`, so byte-buddy came back UNKNOWN;
 * an unresolvable POM was indistinguishable from a licence-less one; it saw
 * only runtime classpaths, so a `compileOnly` OpenSearch SPI would never be
 * examined; and it could not be cached at all.
 *
 * The difference that matters is where the truth lives. Scraped metadata is a
 * claim re-fetched on every build. A `licenses/` directory is a fact, committed
 * once, changed only by a diff someone reads.
 */
abstract class DependencyLicensesTask : DefaultTask() {

    init {
        // ⚠️ Deliberately NOT @CacheableTask, and a deliberate divergence from
        // OpenSearch's version.
        //
        // Gradle treats ~/.gradle/caches/modules-2 as immutable and does not
        // re-hash it, so a jar whose bytes changed under an unchanged version
        // left this task UP-TO-DATE and the mismatch unreported -- measured,
        // not theorised. A supply-chain check that a cache can skip is not a
        // check.
        //
        // The cost of always running it is nil: 567 ms up-to-date versus 543 ms
        // forced, because the time is Gradle startup, not the hashing of ten
        // small jars. Re-measure before reinstating caching.
        outputs.upToDateWhen { false }
    }

    /**
     * The jars to account for.
     *
     * ⚠️ `@InputFiles`, deliberately NOT `@Classpath`. Classpath normalisation
     * ignores entry timestamps and order -- which is precisely what a SHA-1 pin
     * exists to detect. Under `@Classpath`, a jar repacked with identical
     * entries and different timestamps changed its sha1 and the task still
     * reported UP-TO-DATE, then FROM-CACHE. Byte content is the input here.
     */
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NAME_ONLY)
    abstract val dependencies: ConfigurableFileCollection

    // @Internal, not @InputDirectory: Gradle's own validation for a missing
    // input directory fires BEFORE the task action, so the actionable message
    // below was unreachable dead code. Up-to-date tracking is moot here anyway
    // -- see the `outputs.upToDateWhen { false }` above and why.
    @get:Internal
    abstract val licensesDir: DirectoryProperty

    /**
     * Regex -> licence-file prefix, so a family of artifacts shares one licence:
     * `junit-jupiter-api`, `junit-jupiter-engine` and `junit-platform-commons`
     * are all `junit`. Same idea as OpenSearch's `mapping` block.
     */
    @get:Input
    @get:Optional
    abstract val mappings: MapProperty<String, String>

    /** A stamp, so the task has an output and Gradle can call it up to date. */
    @get:OutputFile
    abstract val stamp: RegularFileProperty

    /**
     * SPDX identifiers this project cannot ship (build.md rule 2), matched
     * EXACTLY against the id declared in licenses/SPDX.txt.
     *
     * ⚠️ Not matched against licence prose. EPL-2.0's text names "GNU General
     * Public License" in its Secondary License clause, so a prose scan rejects
     * JUnit -- and a substring scan for "GPL-2.0" also rejects LGPL-2.0, which
     * is a different licence. The declared identifier is the claim; the text is
     * attribution.
     */
    @get:Input
    abstract val denied: org.gradle.api.provider.SetProperty<String>

    @TaskAction
    fun check() {
        val dir = licensesDir.get().asFile
        if (!dir.isDirectory) {
            throw GradleException(
                "$dir does not exist. Every dependency needs a pinned sha and a " +
                    "committed licence; run `./gradlew updateShas` to create the shas, " +
                    "then add <prefix>-LICENSE.txt for each new dependency."
            )
        }

        // prefix -> SPDX id. Adding a dependency means adding a line here, which
        // is a decision someone makes and a reviewer sees.
        val manifestFile = File(dir, "SPDX.txt")
        if (!manifestFile.exists()) {
            throw GradleException("$manifestFile is missing. It declares the SPDX id of each licence.")
        }
        val manifest = manifestFile.readLines()
            .map { it.substringBefore('#').trim() }
            .filter { it.isNotEmpty() }
            .associate { line ->
                val parts = line.split(Regex("\\s+"), limit = 2)
                if (parts.size != 2) throw GradleException("SPDX.txt: expected '<prefix> <SPDX-ID>', got: $line")
                parts[0] to parts[1]
            }

        val problems = mutableListOf<String>()
        val usedPrefixes = mutableSetOf<String>()
        val seenSha = mutableSetOf<String>()
        val seenLicence = mutableSetOf<String>()

        for (jar in dependencies.files.filter { it.name.endsWith(".jar") }.sortedBy { it.name }) {
            val shaFile = File(dir, jar.name + ".sha1")
            if (!shaFile.exists()) {
                problems += "missing sha for ${jar.name} -- run ./gradlew updateShas"
            } else {
                seenSha += shaFile.name
                val actual = sha1(jar)
                val expected = shaFile.readText().trim()
                if (actual != expected) {
                    // The artifact changed under a version we already reviewed.
                    problems += "sha mismatch for ${jar.name}: expected $expected, got $actual. " +
                        "The published artifact changed. Do NOT run updateShas until you know why."
                }
            }

            val prefix = prefixFor(jar.name)
            usedPrefixes += prefix
            val licence = File(dir, "$prefix-LICENSE.txt")
            if (!licence.exists()) {
                problems += "missing ${licence.name} for ${jar.name} -- commit the licence text"
            } else {
                seenLicence += licence.name
            }
            val spdx = manifest[prefix]
            if (spdx == null) {
                problems += "SPDX.txt has no entry for '$prefix' (needed by ${jar.name}). " +
                    "Add: $prefix <SPDX-ID>"
            } else if (spdx in denied.get()) {
                problems += "${jar.name} is $spdx, which this project cannot ship -- build.md rule 2. " +
                    "Replace the dependency, or record an exception in baselines/licenses.txt."
            }
        }

        // A sha with no jar means a dependency was removed and its pin left
        // behind. Harmless today, and the reason a stale pin survives a version
        // bump unnoticed tomorrow.
        dir.listFiles { f: File -> f.name.endsWith(".sha1") }?.forEach {
            if (it.name !in seenSha) problems += "unused sha file ${it.name} -- delete it"
        }
        manifest.keys.forEach {
            if (it !in usedPrefixes) problems += "SPDX.txt names '$it', which no dependency uses -- delete it"
        }

        if (problems.isNotEmpty()) {
            throw GradleException(
                "dependency licences (${problems.size} problem(s)):\n" +
                    problems.joinToString("\n") { "  - $it" }
            )
        }
        stamp.get().asFile.apply { parentFile.mkdirs() }
            .writeText("ok ${dependencies.files.count { it.name.endsWith(".jar") }} jar(s)\n")
    }

    private fun prefixFor(jarName: String): String {
        // Strip only the trailing `-<version>` segment. A greedy
        // `-\d+(\.\d+)*.*$` turned `log4j-1.2-api-2.24.1` into `log4j`,
        // attributing it to whatever licence `log4j` happens to name.
        // Walk segments from the end to the last one that starts with a digit;
        // everything before it is the name. A single lastIndexOf left the
        // version in place for a classified jar (`foo-1.2-sources`), and a
        // greedy regex turned `log4j-1.2-api-2.24.1` into `log4j`.
        val parts = jarName.removeSuffix(".jar").split('-')
        val versionAt = parts.indexOfLast { it.firstOrNull()?.isDigit() == true }
        val base = if (versionAt > 0) parts.subList(0, versionAt).joinToString("-")
                   else parts.joinToString("-")
        for ((pattern, name) in mappings.getOrElse(emptyMap())) {
            if (Regex(pattern).matches(base)) return name
        }
        return base
    }

    private fun sha1(f: File): String =
        MessageDigest.getInstance("SHA-1").digest(f.readBytes())
            .joinToString("") { "%02x".format(it) }
}
