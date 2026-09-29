// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

/**
 * Which of a SPEC's numbered acceptance criteria have no evidence line in
 * its VERIFIED.md -- the pure half of `checkMilestoneVerified`.
 *
 * ⚠️ EVIDENCE IS READ ONLY FROM VERIFIED.md's CRITERIA SECTION (M11.20, H15):
 * its `## Acceptance criteria` section. Read from the whole file, the first
 * `N. ` line ANYWHERE answered for criterion N, so a numbered list further
 * down -- a review's findings, a re-plan -- could stand in for a deleted
 * criterion.
 *
 * ⚠️ AND IT DOES NOT FAIL OPEN (M12.14, H15): a VERIFIED.md with no such
 * section is refused rather than read from its preamble, and one numbering a
 * criterion twice is refused, because only the first line numbered N was ever
 * read and the second went unchecked.
 */
object MilestoneEvidence {
    private val EVIDENCE = Regex("(#|\\.sh|\\bgradlew\\b|Test\\b|NOT-RUN|OBSERVED-NOT)")

    /** The numbers of `spec`'s criteria that `verified` gives no evidence for. */
    fun unevidenced(spec: String, verified: String): List<String> {
        val criteria = numbered(section(spec)
            ?: throw IllegalArgumentException("SPEC.md has no Acceptance criteria section"))
        require(criteria.isNotEmpty()) { "SPEC.md has no numbered acceptance criteria" }
        val evidence = section(verified)
            ?: throw IllegalArgumentException("VERIFIED.md has no Acceptance criteria section")
        numbered(evidence).groupingBy { it }.eachCount().filterValues { it > 1 }.keys.firstOrNull()
            ?.let { throw IllegalArgumentException("VERIFIED.md numbers criterion $it more than once") }
        return criteria.filter { number ->
            val line = Regex("(?m)^\\s*${Regex.escape(number)}\\.\\s+(.+)$").find(evidence)
                ?.groupValues?.get(1)
            line == null || !EVIDENCE.containsMatchIn(line)
        }
    }

    /** How many criteria `spec` declares. */
    fun criteriaCount(spec: String): Int = numbered(section(spec) ?: "").size

    private fun section(text: String): String? =
        Regex("(?ms)^## Acceptance criteria\\s*(.*?)(?=^## |\\z)").find(text)?.groupValues?.get(1)

    private fun numbered(section: String): List<String> =
        Regex("(?m)^\\s*(-?\\d+)\\.\\s+(.+)$").findAll(section).map { it.groupValues[1] }.toList()
}
