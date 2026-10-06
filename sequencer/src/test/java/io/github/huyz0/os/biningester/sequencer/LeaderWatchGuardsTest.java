// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A look survives a bug, and a term this pod departed is never asked again
 * (M13.27n review round 1, P3, T5).
 */
class LeaderWatchGuardsTest {

    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("me", "uid-me", "az-b", "http://me:1");

    private static TermJoiner joiner(TermJoiner.Transport transport) throws IOException {
        return new TermJoiner(SELF, transport,
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()), new JoinedTerms(),
                () -> FastFrame.Held.NONE);
    }

    @Test
    void aLOOKThatHitsAnUncheckedFailureReturnsNothing() throws Exception {
        LeaderWatch watch = new LeaderWatch(SELF.podUid(), () -> {
            throw new IllegalStateException("a backend's bug");
        }, joiner((endpoint, frame) -> new byte[0]), () -> false);

        assertThat(watch.look()).isEmpty();
    }

    @Test
    void aTERMThisPodDepartedIsNotAskedAgain() throws Exception {
        List<String> sent = new ArrayList<>();
        LeaderWatch watch = new LeaderWatch(SELF.podUid(),
                () -> new Lease(3, "l", "uid-l", "http://l:1", 1_000L),
                joiner((endpoint, frame) -> {
                    FastFrame.Header asked = FastFrame.header(frame);
                    sent.add(endpoint);
                    return FastFrame.encode(asked.epoch(), asked.targetUid(), asked.senderUid(),
                            new FastFrame.Refused(FastFrame.Reason.DEPARTING, Optional.empty(),
                                    "departed"));
                }), () -> false);

        watch.look();
        watch.look();

        assertThat(sent).as("departed: one JOIN, never again").hasSize(1);
    }
}
