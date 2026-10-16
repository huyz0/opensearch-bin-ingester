// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.INDEX;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover over many streams writes its recovery as chain entries one per
 * committed batch, their number independent of the stream count, each
 * stream's commits and voids in one of them (SPEC criterion 13; M13.33u6;
 * non-negotiable 6): counted by the store's request counter as the stream
 * count grows.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverManyStreamsTest {

    private static final int MANY = 200;

    /** {@code x} gone at {@code q = 1}: every stream commits m's entry at 0, then voids to B. */
    private static long chainAppends(int streams, List<Recovery.VoidRange> voids,
            List<String> recovered) throws Exception {
        List<FastJournalRecord.Entry> held = new ArrayList<>();
        for (int p = 0; p < streams; p++) {
            held.add(entry(new RunKey(INDEX, p), 1, 1, 0));
        }
        try (TakeoverRig rig = new TakeoverRig(1, 0, Map.of("uid-m", held))) {
            rig.partitions = streams;
            long before = rig.store.putPurposeCounts().commitPuts();

            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            long appends = rig.store.putPurposeCounts().commitPuts() - before;
            for (int p = 0; p < streams; p++) {
                assertThat(rig.term.committedNext(new RunKey(INDEX, p)))
                        .isEqualTo(FastLeaderTerm.STREAM_BOUND);
            }
            voids.addAll(rig.voids);
            recovered.addAll(rig.recovered);
            return appends;
        }
    }

    @Test
    void aTakeoverOverMANYStreamsAppendsAsManyChainEntriesAsOverOne() throws Exception {
        List<Recovery.VoidRange> oneVoids = new ArrayList<>();
        List<Recovery.VoidRange> manyVoids = new ArrayList<>();
        List<String> oneRecovered = new ArrayList<>();
        List<String> manyRecovered = new ArrayList<>();

        long one = chainAppends(1, oneVoids, oneRecovered);
        long many = chainAppends(MANY, manyVoids, manyRecovered);

        assertThat(one).as("one committed batch, one chain entry").isEqualTo(1);
        assertThat(many).as("independent of the stream count").isEqualTo(one);
        List<Recovery.VoidRange> expected = new ArrayList<>();
        for (int p = 0; p < MANY; p++) {
            expected.add(new Recovery.VoidRange(new RunKey(INDEX, p), 1,
                    FastLeaderTerm.STREAM_BOUND));
        }
        assertThat(manyVoids).as("each stream's void in that one entry")
                .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(manyRecovered).as("each stream's commit in it, from one upload")
                .hasSize(MANY).allMatch(r -> r.startsWith("seg-r1:0+1@"));
    }
}
