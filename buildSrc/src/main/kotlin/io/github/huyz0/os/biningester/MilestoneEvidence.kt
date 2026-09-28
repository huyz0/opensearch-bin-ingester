// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

/**
 * Which of a SPEC's numbered acceptance criteria have no evidence line in
 * its VERIFIED.md -- the pure half of `checkMilestoneVerified`.
 *
 * ⚠️ EVIDENCE IS READ ONLY FROM VERIFIED.md's CRITERIA SECTION (M11.20, H15):
 * its `## Acceptance criteria` section when it has one, else everything
 * before its first `## ` heading. Read from the whole file, the first `N. `
 * line ANYWHERE answered for criterion N, so a numbered list further down --
 * a review's findings, a re-plan -- could stand in for a deleted criterion.
 */
object MilestoneEvidence {
    private val EVIDENCE = Regex("(#|\\.sh|\\bgradlew\\b|Test\\b|NOT-RUN|OBSERVED-NOT)")

    /** The numbers of `spec`'s criteria that `verified` gives no evidence for. */
    fun unevidenced(spec: String, verified: String): List<String> {
        val criteria = numbered(section(spec)
            ?: throw IllegalArgumentException("SPEC.md has no Acceptance criteria section"))
        require(criteria.isNotEmpty()) { "SPEC.md has no numbered acceptance criteria" }
        val evidence = section(verified) ?: verified.substringBefore("\n## ")
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
