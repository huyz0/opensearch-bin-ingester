// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

/**
 * The commit message's own rules, as a function of its text and the backlog's
 * (M13.19), so they are tested without a repository.
 *
 * - The subject starts with a task ID the backlog names (git.md rule 1).
 * - ⚠️ THE BODY STATES ITS COST (M13.19, M12 harvest R17; cost.md): a line
 *   starting `Cost:`, and saying something -- `Cost: none` when it is none. A
 *   reviewer instruction to "remember the cost line" was the weakest
 *   enforcement there is; this is a predicate on the file (gate-design rung 3).
 *
 * Merges, reverts and fixups are exempt from both, as they were from the first.
 */
object CommitMessage {

    private val TASK_ID = Regex("^M-?\\d+\\.\\d+")
    private val COST_LINE = Regex("^Cost:[ \\t]*\\S.*")

    /** Why [message] may not be committed against [backlog]; empty if it may. */
    fun failures(message: String, backlog: String): List<String> {
        val lines = message.replace("\r\n", "\n").lines()
        val subject = lines.firstOrNull().orEmpty()
        if (subject.startsWith("Merge ") || subject.startsWith("Revert ") || subject.startsWith("fixup!")) {
            return emptyList()
        }
        val failures = mutableListOf<String>()
        val id = TASK_ID.find(subject)?.value
        if (id == null || !backlog.contains(id)) {
            failures += "commit subject must start with a task ID present in the backlog: $subject"
        }
        if (lines.drop(1).none { COST_LINE.matches(it) }) {
            failures += "commit body must state its cost on a line starting `Cost:` -- " +
                "`Cost: none` when it is none (M13.19, cost.md)"
        }
        return failures
    }
}
