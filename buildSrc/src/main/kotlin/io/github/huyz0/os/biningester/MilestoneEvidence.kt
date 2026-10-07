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
 *
 * ⚠️ AND A HARVEST ENUMERATION MUST BE WHOLE (M13.40): every harvest ID the
 * SPEC's obligations table lists individually -- its `Harvest` row, a range
 * or a parenthetical naming none -- must lead a row of a VERIFIED.md table
 * headed `Harvest`, or the milestone closes a row nobody disposed of.
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
        unenumerated(spec, verified).takeIf { it.isNotEmpty() }?.let {
            throw IllegalArgumentException("VERIFIED.md's harvest enumeration is missing ${it.joinToString()}")
        }
        return criteria.filter { number ->
            val line = Regex("(?m)^\\s*${Regex.escape(number)}\\.\\s+(.+)$").find(evidence)
                ?.groupValues?.get(1)
            line == null || !EVIDENCE.containsMatchIn(line)
        }
    }

    /** The harvest IDs `spec`'s obligations table lists individually that `verified` does not enumerate. */
    fun unenumerated(spec: String, verified: String): List<String> {
        val listed = tables(spec, "Obligation").flatten()
            .map { it.first() }
            .filter { it.startsWith("harvest", ignoreCase = true) }
            .flatMap { cell ->
                val bare = cell.replace(Regex("\\([^)]*\\)"), " ")
                    .replace(Regex("\\b[A-Z]\\d+\\s*[–-]\\s*[A-Z]?\\d+\\b"), " ")
                ID.findAll(bare).map { it.value }.toList()
            }
            .distinct()
        if (listed.isEmpty()) {
            return listed
        }
        // ⚠️ ONE ENUMERATION (M13.40 review P1): a closing VERIFIED.md carries
        // more than one milestone's harvest and IDs repeat across milestones,
        // so a second table headed Harvest leaves it guessing which is this one.
        val enumerations = tables(verified, "Harvest")
        require(enumerations.size <= 1) { "VERIFIED.md has more than one harvest enumeration" }
        // ⚠️ THE BARE ID, or the ID and a parenthetical: a finding's `R3-P1` is
        // not R3.
        val enumerated = enumerations.flatten()
            .mapNotNull { Regex("^([A-Z]\\d+)(\\s*\\(.*\\))?$").find(it.first())?.groupValues?.get(1) }
            .toSet()
        return listed.filter { it !in enumerated }
    }

    private val ID = Regex("\\b[A-Z]\\d+\\b")

    /**
     * The body rows, as cells stripped of emphasis, of each markdown table whose
     * first header cell is `header` -- outside code fences, which hold examples.
     */
    private fun tables(text: String, header: String): List<List<List<String>>> {
        val tables = mutableListOf<MutableList<List<String>>>()
        var current: MutableList<List<String>>? = null
        var headerSeen = false
        var fenced = false
        for (line in text.lines()) {
            val trimmed = line.trim()
            if (trimmed.startsWith("```")) {
                fenced = !fenced
            }
            if (fenced || !trimmed.startsWith("|")) {
                current = null
                headerSeen = false
                continue
            }
            val cells = trimmed.trim('|').split("|").map { it.trim().trim('*').trim() }
            if (!headerSeen) {
                headerSeen = true
                current = if (cells.firstOrNull().equals(header, ignoreCase = true)) {
                    mutableListOf<List<String>>().also { tables.add(it) }
                } else {
                    null
                }
                continue
            }
            if (!cells.all { it.matches(Regex("-*:?-+:?")) || it.isEmpty() }) {
                current?.add(cells)
            }
        }
        return tables
    }

    /** How many criteria `spec` declares. */
    fun criteriaCount(spec: String): Int = numbered(section(spec) ?: "").size

    private fun section(text: String): String? =
        Regex("(?ms)^## Acceptance criteria\\s*(.*?)(?=^## |\\z)").find(text)?.groupValues?.get(1)

    private fun numbered(section: String): List<String> =
        Regex("(?m)^\\s*(-?\\d+)\\.\\s+(.+)$").findAll(section).map { it.groupValues[1] }.toList()
}
