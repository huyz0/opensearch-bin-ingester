// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFence;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.FastTermStart;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A forwarded switch to the default path (M13.34ad; ADR-0082 §2): a writer
 * that told its leader an index is fast tells it when that index leaves fast
 * mode, and the leader takes the {@code wal=false} registration -- but only
 * of an index it holds fast under the same UUID. Before this a follower's
 * stale fast REGISTER, arriving after the leader's own switch, held the index
 * fast at the leader for good: the follower's later switch was never told.
 */
class FastRegisterSwitchServedTest {

    private static final String PREFIX = "bins/c";
    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final IndexRegistration DEFAULT = IndexRegistration.unsplit(UUID_B64,
            "orders", 1);
    private static final IndexRegistration FAST = DEFAULT.withFastSettings(250, true, 2);
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("pod1", "uid-pod1", "az-a", "pod1:9443");
    private static final MonotonicClock MONO = () -> 0L;

    /** A serving term led by pod1, its fast term opened: pod1 its one rostered member. */
    private static LocalSequencer leading(MemoryBinStore store) throws Exception {
        Clock wall = Clock.systemUTC();
        LocalSequencer term = LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                new LeaseConfig(PREFIX, "pod1", "", Duration.ofSeconds(10),
                        Duration.ofSeconds(3)), wall), 8).orElseThrow();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, PREFIX),
                new EmptyTermCloser(store, PREFIX)::close,
                new FastLeaseFence(Duration.ofSeconds(10), wall, MONO), SELF, Map::of)
                .open(term.epoch(), term::close).orElseThrow();
        term.attach(new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, PREFIX, term.epoch(), MONO,
                        Duration.ofMillis(250)), nanos -> { }),
                term::committedNext, store, PREFIX, MONO, Duration.ofMillis(250)));
        return term;
    }

    private static FastFrame.Header from(String sender) {
        return new FastFrame.Header(FastFrame.KIND_REGISTER, 1, sender, "uid-pod1");
    }

    @Test
    void aFASTIndexsSwitchToTheDefaultPathIsTaken() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            catalog.register(FAST);
            try {
                FastFrame.Body answer = FastLeaderFrames.answerRegister(term, catalog,
                        from("uid-pod1"), new FastFrame.Register(DEFAULT));

                assertThat(answer)
                        .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
                assertThat(catalog.resolve("orders")).hasValue(DEFAULT);
            } finally {
                term.close();
            }
        }
    }

    @Test
    void aSWITCHTheLeaderAlreadyMadeIsTakenNotRefused() throws Exception {
        // ⚠️ its review round 1, P1: refused, the writer re-sent it every second.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            catalog.register(DEFAULT);
            try {
                assertThat(FastLeaderFrames.answerRegister(term, catalog, from("uid-pod1"),
                        new FastFrame.Register(DEFAULT)))
                        .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
                assertThat(catalog.resolve("orders")).hasValue(DEFAULT);
            } finally {
                term.close();
            }
        }
    }

    @Test
    void aDEFAULTRegistrationOfAnotherIndexOfTheSameNameIsRefused() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            catalog.register(FAST);
            IndexRegistration recreated = IndexRegistration.unsplit("Qk1rUnfdRyCrk1L0DLaXEg",
                    "orders", 1);
            try {
                assertThat(((FastFrame.Refused) FastLeaderFrames.answerRegister(term, catalog,
                        from("uid-pod1"), new FastFrame.Register(recreated))).reason())
                        .isEqualTo(FastFrame.Reason.NOT_FAST);
                assertThat(catalog.resolve("orders")).as("the leader's writes stay put")
                        .hasValue(FAST);
            } finally {
                term.close();
            }
        }
    }
}
