// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import java.util.Map;

/**
 * How a chain entry moves each stream's next offset (moved out of
 * {@code ChainReplay} by M13.25h, unchanged, at that file's ceiling).
 */
final class ChainFold {

    private ChainFold() {
    }

    /**
     * ⚠️ THE ONE PLACE a delta becomes an offset. It lived in
     * {@code ChainReplay} AND in {@code CommitLog.apply} until review pointed
     * out that the split's stated principle — do not re-derive wire semantics
     * in two places — had been applied to the key grammar and not to this.
     *
     * <p>{@code Math::max} rather than assignment, because entries arrive in
     * ascending sequence within a chain but a CROSSING merges an older chain's
     * offsets into a newer one's; taking the later value unconditionally would
     * rewind the stream, which is I2.
     */
    static void fold(ChainEntry entry, Map<RunKey, Long> into) {
        switch (entry) {
            case CommitDelta delta -> {
                // ⚠️ `allRuns`, deliberately: an offset is a STREAM fact, not a
                // segment fact, so this is the one reader that genuinely does not
                // care which object holds the records. Everything that DELIVERS
                // records pairs each run with its own segment instead — see
                // SubscriptionHub and ADR-0032.
                for (RunCommit run : delta.allRuns()) {
                    into.merge(run.key(), run.lastOffset() + 1, Math::max);
                }
            }
            case Recovery recovery -> {
                // ⚠️ A VOID IS A COMMITTED HOLE (ADR-0082 §5): it moves its stream
                // past it, so no later commit assigns inside it; never backwards (I2).
                recovery.delta().ifPresent(delta -> fold(delta, into));
                for (Recovery.VoidRange v : recovery.voids()) {
                    into.merge(v.key(), v.toOffsetExclusive(), Math::max);
                }
            }
            case Seal ignored -> { }
            case Continue ignored -> { }
        }
    }
}
