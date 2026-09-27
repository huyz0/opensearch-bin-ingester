// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.IOException;
import java.util.Objects;

/**
 * The declaration that a LIST is RECOVERY, which the cost governor never
 * refuses (M10.10, ADR-0075).
 *
 * <p>⚠️ DECLARED BY THE CALLER, NEVER INFERRED FROM A KEY. A reinstated poll of
 * the commit log lists exactly the prefix chain-end recovery lists, so a
 * prefix rule would exempt the runaway this governor exists for. Four callers
 * declare it -- chain-end recovery, chain replay, the takeover backfill and
 * the inbox drain -- each bounded by its trigger instead (cost.md rule 2b,
 * ADR-0058); a fifth is a change to ADR-0075.
 *
 * <p>⚠️ A {@link ScopedValue}, SO IT DOES NOT CROSS A THREAD BOUNDARY: a caller
 * that starts its own thread must bind the scope inside that thread's body,
 * or its LISTs run undeclared and are governed.
 */
public final class GovernorScope {

    /** A unit of store work that may throw. */
    @FunctionalInterface
    public interface IoCallable<T> {
        T call() throws IOException;
    }

    private static final ScopedValue<Boolean> RECOVERY = ScopedValue.newInstance();

    private GovernorScope() {
    }

    /** Runs {@code work} with every LIST it issues declared as recovery. */
    public static <T> T recovery(IoCallable<T> work) throws IOException {
        Objects.requireNonNull(work, "work");
        return ScopedValue.where(RECOVERY, Boolean.TRUE).call(work::call);
    }

    /** Whether the current thread is inside a declared recovery scope. */
    public static boolean inRecovery() {
        return RECOVERY.isBound();
    }
}
