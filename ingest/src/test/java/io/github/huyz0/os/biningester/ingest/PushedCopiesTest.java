// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The pushed copies a pod keeps for a proxied read (M13.30l; ADR-0081 §6):
 * one released once its stream's release-below offset passes its end, never
 * before; and the bytes held bounded, oldest first, one over the bound never
 * kept.
 */
class PushedCopiesTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(1, 1), 1);

    @Test
    void aCOPYIsReleasedOnlyOnceTheOffsetPassesItsEnd() {
        PushedCopies copies = new PushedCopies(1_000);
        copies.put("s10", S, 10, 5, new byte[] {1});
        copies.put("s15", S, 15, 5, new byte[] {2});
        copies.put("t10", T, 10, 5, new byte[] {3});

        copies.release(S, 19);

        assertThat(copies.copy("s10")).as("[10, 15) passed").isEmpty();
        assertThat(copies.copy("s15")).as("[15, 20) not wholly").isPresent();
        assertThat(copies.copy("t10")).as("another stream").isPresent();
        assertThat(copies.heldBytes()).isEqualTo(2);
    }

    @Test
    void theBYTESHeldAreBoundedOldestFirst() {
        PushedCopies copies = new PushedCopies(10);
        copies.put("a", S, 0, 1, new byte[4]);
        copies.put("b", S, 1, 1, new byte[4]);
        copies.put("c", S, 2, 1, new byte[4]);
        copies.put("huge", S, 3, 1, new byte[11]);

        assertThat(copies.copy("a")).as("the oldest, evicted").isEmpty();
        assertThat(copies.copy("b")).isPresent();
        assertThat(copies.copy("c")).isPresent();
        assertThat(copies.copy("huge")).as("over the bound alone").isEmpty();
        assertThat(copies.heldBytes()).isEqualTo(8);
    }
}
