// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * A write that names a partition this index does not have, or an index whose
 * shard this ingester cannot compute (M6.6, FR-13).
 *
 * <p>⚠️ IT EXISTS SO THE ADAPTER CAN ANSWER 400 WITHOUT CATCHING
 * {@link IllegalArgumentException}. IAE is also how the append path reports its
 * own invariant failures -- {@code AppendResult} and {@code CommitLog} both
 * throw it -- and {@code BulkService}'s javadoc says in as many words why
 * mapping those to a client error is worse than a 500: it tells the producer
 * its batch was permanently refused, so the producer drops it and an
 * implementation bug becomes data loss. This type names the one condition that
 * really is the producer's fault and really is permanent.
 *
 * <p>⚠️ IT STILL EXTENDS IAE, so every existing caller and test that treats an
 * unplaceable write as an illegal argument keeps working. What it adds is a
 * name the adapter can catch WITHOUT catching the ones it must not.
 *
 * <p>⚠️ PERMANENT, NEVER PENDING. An index nobody has registered yet is NOT
 * this: it waits in the pending pool, because ADR-0015's Consequences require
 * that a producer starting before the plugin connects is not punished for a
 * race it cannot see. That refusal, when it finally comes, is
 * {@link RegistrationTimeoutException}.
 */
public final class PlacementRefusedException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public PlacementRefusedException(String message) {
        super(message);
    }
}
