// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a backfill adds to the in-memory chain, and when it may claim the
 * floor (M8.42).
 */
class ChainMemoryBackfillTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, List.of(new SegmentCommit("seg-" + sequence,
                List.of(new RunCommit(STREAM, 1, sequence)),
                new SegmentCommit.Attribution("poda", "i1", sequence))));
    }

    private static ChainGc.DeltaAt at(long epoch, long sequence) {
        return new ChainGc.DeltaAt(epoch, sequence, delta(sequence));
    }

    @Test
    void aBACKFILLIsPREPENDEDOldestFirstAndOnlyBELOWTheFirstHeld() {
        ChainMemory chain = new ChainMemory(100);
        chain.record(4, delta(3));
        chain.record(4, delta(4));

        chain.backfill(List.of(at(3, 1), at(4, 2), at(4, 3), at(4, 9)));

        assertThat(chain.snapshot().deltas()).extracting(CommitDelta::sequence)
                .as("⚠️ (4, 3) IS ALREADY HELD and (4, 9) is past it: neither is added, "
                        + "so a backfill racing a commit cannot duplicate or reorder")
                .containsExactly(1L, 2L, 3L, 4L);
        assertThat(chain.snapshot().firstEpoch()).isEqualTo(3);
        assertThat(chain.snapshot().fromFloor()).isTrue();
    }

    @Test
    void aChainNeverBackfilledDoesNOTClaimTheFloor() {
        ChainMemory chain = new ChainMemory(100);
        chain.record(4, delta(1));

        assertThat(chain.snapshot().fromFloor()).isFalse();
    }

    @Test
    void theCAPEvictingABackfilledDeltaWITHDRAWSTheClaim() {
        ChainMemory chain = new ChainMemory(3);
        chain.record(4, delta(5));

        chain.backfill(List.of(at(4, 1), at(4, 2), at(4, 3)));

        assertThat(chain.snapshot().fromFloor())
                .as("⚠️ A KEEP LIST MISSING ITS OLDEST DELTA IS THE FAIL-OPEN ONE")
                .isFalse();
    }

    @Test
    void aRESETForgetsTheClaim() {
        ChainMemory chain = new ChainMemory(100);
        chain.backfill(List.of(at(3, 1)));
        chain.reset();

        assertThat(chain.snapshot().fromFloor())
                .as("a chain recovered again has not been backfilled again").isFalse();
    }
}
