// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.ChainEntry;
import binjava.format.Seal;
import java.io.IOException;
import java.util.Map;

/**
 * WHERE a chain ends, without reading what it holds.
 *
 * <p>⚠️ Split out of {@link ChainReplay} when M4.9's bounded crossing pushed
 * that file past the 500-line limit — code-structure.md rule 1, split it rather
 * than raise the limit. The seam is the one this method's own javadoc already
 * declared: finding a chain's END is a different job from REPLAYING it, needs
 * none of the replay's accumulated state, and has exactly one caller.
 */
final class ChainEnd {

    private ChainEnd() {
    }

    /**
     * Where a chain ENDS, without reading what it holds.
     *
     * <p>⚠️ FOR A WRITER THAT WILL ONLY SEAL. A taking-over leader needs the
     * predecessor's last slot and nothing else, and a full replay would spend
     * one GET per entry of that whole term. The LIST alone gives the answer
     * because the keys sort numerically; only the last entry is read, to learn
     * whether the chain is already sealed.
     *
     * <p>⚠️ The returned offsets are EMPTY on purpose. A log recovered this way
     * must never commit — that would reassign offsets from 0, which is I2.
     */
    static ChainReplay.Result of(BinStore store, String prefix, long epoch) throws IOException {
        String lastKey = null;
        String startAfter = null;
        LogKeys keys = new LogKeys(prefix, epoch);
        String logPrefix = keys.logPrefix();
        while (true) {
            ListPage page = store.list(logPrefix, startAfter, 1000);
            for (ObjectStat stat : page.objects()) {
                // ⚠️ SKIP, never STOP. A checkpoint sorts after every entry, so
                // breaking here would be indistinguishable against one — but a
                // non-entry key that sorts in the MIDDLE (a half-written
                // `.delta.tmp`) would end the chain early and hand a taking-over
                // leader a `nextSequence` inside its predecessor's history.
                if (!keys.isEntryKey(stat.key())) {
                    continue;
                }
                lastKey = stat.key();
            }
            if (page.nextStartAfter().isEmpty()) {
                break;
            }
            startAfter = page.nextStartAfter().get();
        }
        if (lastKey == null) {
            return new ChainReplay.Result(Map.of(), 0, null);
        }
        ChainEntry last = ChainReplay.read(store, lastKey);
        // ⚠️ Set directly rather than by applying: applying a DELTA here would
        // populate offsets from one arbitrary entry, which is worse than leaving
        // them empty because it looks like recovery and is not.
        return new ChainReplay.Result(Map.of(), last.sequence() + 1,
                last instanceof Seal s ? s : null);
    }

}
