// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.PushedCopies;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader's own pushed copies (M13.30l): it sends itself no RELEASE, so
 * each is dropped once its own chain's committed next offset for the stream
 * passes it -- and only then.
 */
class FastOwnCopiesTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(1, 1), 1);

    @Test
    void theLEADERsOwnCopiesAreDroppedBelowItsChainsCommittedNext() {
        PushedCopies copies = new PushedCopies(1_000);
        copies.put("s0", S, 0, 2, new byte[] {1});
        copies.put("s2", S, 2, 2, new byte[] {2});
        copies.put("t0", T, 0, 2, new byte[] {3});

        FastLeaderFrames.releaseOwnCopies(copies, Map.of(S, 2L, T, 1L)::get);

        assertThat(copies.copy("s0")).as("committed").isEmpty();
        assertThat(copies.copy("s2")).isPresent();
        assertThat(copies.copy("t0")).as("not wholly committed").isPresent();
    }
}
