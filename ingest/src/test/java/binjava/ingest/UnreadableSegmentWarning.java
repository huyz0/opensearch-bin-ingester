// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * The question {@link UndeliverablePushTest} asks of a captured log record.
 *
 * <p>⚠️ ITS OWN FILE SO IT CAN HAVE A RED. The predicate started as a private
 * method of the test that uses it, and review measured `return true;` --
 * exactly the `logged.isEmpty()` wait it replaced -- surviving the whole suite.
 * A T0 case over it is the cheap pin, but `tdd-red.sh` binds a red record to
 * the sha256 of the TEST file, so a subject living in that same file cannot be
 * stubbed and restored without moving the hash out from under its own record.
 * Separating them is what makes the red mechanical rather than argued.
 *
 * <p>⚠️ AND IT HOLDS THE KEY, so the fixture that mints the unreadable segment
 * and the assertion that looks for it cannot disagree about the spelling.
 */
final class UnreadableSegmentWarning {

    /** A segment key no pod ever wrote, so the store is certain not to hold it. */
    static final String KEY = "seg-no-pod-ever-wrote-this";

    private UnreadableSegmentWarning() {
    }

    /**
     * Whether {@code record} is the WARNING naming the segment that could not be read.
     *
     * <p>⚠️ BOTH HALVES MATTER. The key alone would accept a line demoted below
     * WARNING, which an operator on an unconfigured JVM never sees; the level
     * alone would accept any warning in the JVM, including the lease renewer's.
     */
    static boolean names(LogRecord record) {
        return record.getLevel() == Level.WARNING
                && String.valueOf(record.getMessage()).contains(KEY);
    }
}
