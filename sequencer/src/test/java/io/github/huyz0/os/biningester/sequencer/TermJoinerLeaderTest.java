// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A join records the term's leader with the term (M13.28a review round 1,
 * T1): a holder journals a REPLICA only from it, so a join that recorded no
 * leader would have every REPLICA refused.
 */
class TermJoinerLeaderTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-2", "az-b", "");
    private static final String LEASE = "p/ctl/lease/0.json";

    @Test
    void aJOINRecordsItsTermsLeader() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(7, "leader", "", 1_000).encode()));
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                "uid-2", new FastFrame.Joined(0, FastFrame.HeldStatus.NONE)),
                EpochFence.start(store, LEASE, Optional.empty()), terms,
                () -> FastFrame.Held.NONE);

        joiner.join(7, "uid-l", "e");

        assertThat(terms.leaderOf(7)).contains("uid-l");
    }

    @Test
    void aJOINRecordsWhereItReachedTheLeader() throws Exception {
        // ⚠️ M13.29c: a writer's batches of the term go to that endpoint.
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(7, "leader", "", 1_000).encode()));
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                "uid-2", new FastFrame.Joined(0, FastFrame.HeldStatus.NONE)),
                EpochFence.start(store, LEASE, Optional.empty()), terms,
                () -> FastFrame.Held.NONE);

        joiner.join(7, "uid-l", "leader:9443");

        assertThat(terms.latest()).hasValue(new JoinedTerms.Leader(7, "uid-l", "leader:9443"));
    }
}
