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
 * The leader takes a forwarded REGISTER into its catalog (M13.83a;
 * ADR-0082 §2): the catalog its term record is fed from -- only while it
 * leads a fast term, only from a pod rostered in it, only a {@code wal=true}
 * index, and never over another index of the same name.
 */
class FastRegisterServedTest {

    private static final String PREFIX = "bins/c";
    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final IndexRegistration ORDERS = IndexRegistration.unsplit(UUID_B64,
            "orders", 1).withFastSettings(250, true, 2);
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
    void aLEADERTakesARosteredWritersRegistrationIntoItsCatalog() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            try {
                FastFrame.Body answer = FastLeaderFrames.answerRegister(term, catalog,
                        from("uid-pod1"), new FastFrame.Register(ORDERS));

                assertThat(answer)
                        .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
                assertThat(catalog.resolve("orders")).hasValue(ORDERS);
            } finally {
                term.close();
            }
        }
    }

    @Test
    void aPODNotRosteredInTheTermIsRefused() throws Exception {
        // ⚠️ its review round 1, P1: a forged or stale REGISTER moves nothing.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            try {
                FastFrame.Body answer = FastLeaderFrames.answerRegister(term, catalog,
                        from("uid-stranger"), new FastFrame.Register(ORDERS));

                assertThat(((FastFrame.Refused) answer).reason())
                        .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
                assertThat(catalog.resolve("orders")).isEmpty();
            } finally {
                term.close();
            }
        }
    }

    @Test
    void aDEFAULTIndexIsRefused() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            try {
                assertThat(((FastFrame.Refused) FastLeaderFrames.answerRegister(term, catalog,
                        from("uid-pod1"), new FastFrame.Register(IndexRegistration.unsplit(
                                UUID_B64, "logs", 1)))).reason())
                        .isEqualTo(FastFrame.Reason.NOT_FAST);
                assertThat(catalog.resolve("logs")).isEmpty();
            } finally {
                term.close();
            }
        }
    }

    @Test
    void anINDEXRecreatedUnderItsOldNameReRegisters() throws Exception {
        // ⚠️ its review round 2, P5 and T4: last wins, as ADR-0015 has it.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer term = leading(store);
            IndexCatalog catalog = new IndexCatalog();
            catalog.register(IndexRegistration.unsplit("Qk1rUnfdRyCrk1L0DLaXEg", "orders", 1)
                    .withFastSettings(250, true, 2));
            try {
                FastLeaderFrames.answerRegister(term, catalog, from("uid-pod1"),
                        new FastFrame.Register(ORDERS));

                assertThat(catalog.resolve("orders")).hasValue(ORDERS);
            } finally {
                term.close();
            }
        }
    }

    @Test
    void aSERVINGTermWithNoFastTermRefusesIt() throws Exception {
        // ⚠️ its review round 2, T5.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer bare = LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                    new LeaseConfig(PREFIX, "pod1", "", Duration.ofSeconds(10),
                            Duration.ofSeconds(3)), Clock.systemUTC()), 8).orElseThrow();
            IndexCatalog catalog = new IndexCatalog();
            try {
                assertThat(((FastFrame.Refused) FastLeaderFrames.answerRegister(bare, catalog,
                        from("uid-pod1"), new FastFrame.Register(ORDERS))).reason())
                        .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
                assertThat(catalog.resolve("orders")).isEmpty();
            } finally {
                bare.close();
            }
        }
    }

    @Test
    void aPODLeadingNoFastTermRefusesItAndRegistersNothing() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LocalSequencer closed = leading(store);
            closed.close();
            IndexCatalog catalog = new IndexCatalog();

            FastFrame.Body answer = FastLeaderFrames.answerRegister(closed, catalog,
                    from("uid-pod1"), new FastFrame.Register(ORDERS));

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(catalog.resolve("orders")).isEmpty();
        }
    }
}
