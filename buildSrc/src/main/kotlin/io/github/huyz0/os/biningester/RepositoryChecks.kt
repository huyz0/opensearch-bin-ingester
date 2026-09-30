// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import java.nio.file.Path
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3

/** One repository check `./gradlew gates` runs: its name, and what it adds to the failures. */
class RepositoryCheck private constructor(
    val name: String,
    val run: (Path, List<Path>, MutableList<String>) -> Unit,
) {
    companion object {
        /**
         * ⚠️ NAMED BY ITS OWN REFERENCE (M13.15 review P1/T2): a name written
         * beside the call could be bound to another predicate unseen; taken
         * from the reference, the name IS the predicate run.
         */
        fun of(check: KFunction3<Path, List<Path>, MutableList<String>, Unit>): RepositoryCheck =
            RepositoryCheck(check.name) { root, files, failures -> check(root, files, failures) }

        /** A check that reads the root alone, not the tracked files. */
        fun ofRoot(check: KFunction2<Path, MutableList<String>, Unit>): RepositoryCheck =
            RepositoryCheck(check.name) { root, _, failures -> check(root, failures) }
    }
}

/**
 * M13.15 (M12 harvest R13): the [RepositoryGateChecks] predicates
 * `RepositoryGatesTask.verify` runs, AS DATA. Their wiring tests used to match
 * the call's text in `verify()`'s source, so a commented-out call, or one moved
 * after the throw, still passed (M11.24 review T4). A test now takes its check
 * from [ALL] by name and runs it on a tree that breaks it: dropped from the
 * list, the lookup fails. Each entry is built from the predicate's reference,
 * which names it, so an entry cannot run one predicate under another's name.
 *
 * `RepositoryChecksTest` runs `verify()` on a scratch repository, so the list
 * is pinned as run, and derives the names the list must hold from
 * [RepositoryGateChecks]' predicates. ⚠️ The task's own private checks are not
 * in the list.
 */
object RepositoryChecks {

    val ALL: List<RepositoryCheck> = listOf(
        RepositoryCheck.ofRoot(RepositoryGateChecks::splitCeilings),
        RepositoryCheck.of(RepositoryGateChecks::ledgerOwnership),
        RepositoryCheck.of(RepositoryGateChecks::singleTooManyRequests),
        RepositoryCheck.of(RepositoryGateChecks::adrReferences),
        RepositoryCheck.ofRoot(RepositoryGateChecks::gateScope),
        RepositoryCheck.of(RepositoryGateChecks::javadocCitations),
        RepositoryCheck.ofRoot(RepositoryGateChecks::portability),
        RepositoryCheck.of(RepositoryGateChecks::metricCardinality),
        RepositoryCheck.of(RepositoryGateChecks::ioSeam),
        RepositoryCheck.of(RepositoryGateChecks::faultStore),
        RepositoryCheck.ofRoot(RepositoryGateChecks::testBudget),
        RepositoryCheck.ofRoot(RepositoryGateChecks::moduleDrift),
        RepositoryCheck.ofRoot(RepositoryGateChecks::moduleDependencies),
    )

    /** The check named [name]; throws if the list has none, or more than one. */
    fun named(name: String): RepositoryCheck = ALL.single { it.name == name }
}
