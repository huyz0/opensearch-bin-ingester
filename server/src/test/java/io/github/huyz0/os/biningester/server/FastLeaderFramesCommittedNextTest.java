// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a fast upload's delta committed (M13.31a review round 2, T7): read
 * from the delta the chain holds, never from what was asked -- a retry folded
 * into a landed delta holds fewer records than it carried.
 */
class FastLeaderFramesCommittedNextTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    @Test
    void eachSTREAMsNextOffsetIsTheEndOfItsRunInTheDelta() {
        CommitDelta delta = new CommitDelta(4, List.of(new SegmentCommit("seg/f",
                List.of(new RunCommit(S, 2, 100), new RunCommit(T, 5, 0)))));

        assertThat(FastLeaderFrames.committedNext(delta)).isEqualTo(Map.of(S, 102L, T, 5L));
    }
}
