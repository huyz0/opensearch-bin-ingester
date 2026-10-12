// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Where a fast index's appends go (M13.29e; ADR-0081 §2): only an index the
 * catalog says is fast, and through the term this pod leads before the
 * newest it joined -- a leader joins no term of its own, and one it joined as
 * a follower before it led is deposed.
 */
class FastRoutingTest {

    private static final String PREFIX = "bins/c";
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("pod1", "uid-pod1", "az-a", "pod1:9443");

    private static LocalSequencer term(MemoryBinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                new LeaseConfig(PREFIX, "pod1", "", Duration.ofSeconds(10),
                        Duration.ofSeconds(3)), Clock.systemUTC()), 8).orElseThrow();
    }

    private static JoinedTerms joinedAsFollower() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(9, 0, "uid-other", "other:9443");
        return joined;
    }

    @Test
    void onlyAnINDEXTheCatalogSaysIsFastIsRouted() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration("nVzgup36TLqWp7VBBREj1w", "orders", List.of(),
                1, 1, 1, 1).withFastSettings(250, true, 1));
        catalog.register(new IndexRegistration("Qk1rUnfdRyCrk1L0DLaXEg", "logs", List.of(),
                1, 1, 1, 1));

        Predicate<String> fast = FastRouting.fastIndices(catalog);

        assertThat(fast.test("orders")).isTrue();
        assertThat(fast.test("logs")).isFalse();
        assertThat(fast.test("unknown")).isFalse();
    }

    @Test
    void theTERMThisPodLeadsIsWrittenThroughBeforeAnyItJoined() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer leading = term(store);
            try {
                assertThat(FastRouting.leaders(() -> leading, joinedAsFollower(), SELF).get())
                        .hasValue(new JoinedTerms.Leader(leading.epoch(), "uid-pod1",
                                "pod1:9443"));
            } finally {
                leading.close();
            }
        }
    }

    @Test
    void aPODLeadingNoTermWritesThroughTheNewestItJoined() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer closed = term(store);
            closed.close();

            assertThat(FastRouting.leaders(() -> closed, joinedAsFollower(), SELF).get())
                    .as("held but no longer serving").hasValue(
                            new JoinedTerms.Leader(9, "uid-other", "other:9443"));
            assertThat(FastRouting.leaders(() -> null, joinedAsFollower(), SELF).get())
                    .as("holding none").hasValue(
                            new JoinedTerms.Leader(9, "uid-other", "other:9443"));
        }
    }
}
