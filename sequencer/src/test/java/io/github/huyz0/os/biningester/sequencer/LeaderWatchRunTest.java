// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The watch's pace: one look per interval, so one lease GET per interval
 * (ADR-0081 §1, amended by M13.27n; its review round 2, T9), stopped by its
 * flag as well as an interrupt (P4).
 */
class LeaderWatchRunTest {

    @Test
    void theWATCHLooksOncePerIntervalUntilStopped() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        List<Duration> slept = new ArrayList<>();
        LeaderWatch watch = new LeaderWatch("uid-me", () -> {
            reads.incrementAndGet();
            return new Lease(1, "me", "uid-me", "http://me:1", 1_000L);
        }, new TermJoiner(new Roster.Incarnation("me", "uid-me", "az-a", "http://me:1"),
                (endpoint, frame) -> new byte[0],
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()),
                new JoinedTerms(), () -> FastFrame.Held.NONE), () -> false);

        watch.run(Duration.ofSeconds(3), slept::add, () -> slept.size() >= 3);

        assertThat(slept).as("each pause the whole interval").containsOnly(Duration.ofSeconds(3))
                .hasSize(3);
        assertThat(reads).as("one read per interval").hasValue(3);
    }
}
