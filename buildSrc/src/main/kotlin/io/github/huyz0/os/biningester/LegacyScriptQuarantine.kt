// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

/**
 * M13.8 (M12 harvest R6): which buildSrc test classes may be quarantined out
 * of `./gradlew -p buildSrc test` into `legacyScriptTest` -- tests that drive
 * a shell or Python script under `scripts/`, and nothing else.
 *
 * ⚠️ A class is refused unless its entry is a plain class name (no wildcard,
 * which the test filter would widen to other classes), it is not a
 * `nativeGateTest` member, its source exists, and its CODE -- comments
 * stripped -- names a `scripts/` `.sh` or `.py` file. Review rounds found each
 * of those holes open in turn.
 *
 * ⚠️ IT DOES NOT TELL A RETIRED SCRIPT FROM A LIVE ONE. Most listed classes
 * drive retired gate scripts; two drive live tools the skills run
 * (`scripts/review.sh`, `scripts/current-milestone.sh`), which fail on the
 * development rig the same way. What keeps a live tool's test out of the list
 * is review of the list, not this guard.
 */
object LegacyScriptQuarantine {

    private val PLAIN_NAME = Regex("[A-Za-z0-9]+")
    private val SCRIPT = Regex("scripts/[A-Za-z0-9_./-]+\\.(sh|py)")

    /** The entries of the quarantine file: its lines, trimmed, without blanks or `#` comments. */
    fun entries(file: String): List<String> =
        file.lines().map { it.trim() }.filter { it.isNotEmpty() && !it.startsWith("#") }

    /**
     * Why each of [entries] may not be quarantined; empty if every one may.
     *
     * @param source a test class's source by its simple name, or null if there is none
     */
    fun refusals(entries: List<String>, natives: Set<String>, source: (String) -> String?):
        List<String> = entries.mapNotNull { entry ->
            val text = if (PLAIN_NAME.matches(entry)) source(entry) else null
            when {
                !PLAIN_NAME.matches(entry) -> "$entry is not a plain class name"
                entry in natives -> "$entry is a nativeGateTest member"
                text == null -> "$entry does not exist"
                !SCRIPT.containsMatchIn(code(text)) -> "$entry drives no script under scripts/"
                else -> null
            }
        }

    /** [source] without its comments, so a script named only in prose does not count. */
    fun code(source: String): String =
        source.replace(Regex("(?s)/\\*.*?\\*/"), "").replace(Regex("//[^\\n]*"), "")
}
