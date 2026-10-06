// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M8's criterion 4: the GC loop runs in the assembled process and a pass with
 * nothing expired costs no request (M8.5, NFR-3, NFR-2).
 *
 * <p>⚠️ **THIS IS THE CRITERION M7 COULD NOT WRITE** (M7.25). A GC pass needs
 * the commit chain, and before M8.3 the only way to get one was the recovery
 * walk -- one LIST per 1,000 deltas plus one GET per delta, every pass, for
 * ever. The assertion is made against the ASSEMBLED graph on its own REAL
 * scheduler, not against a pass handed a list: what it constrains is where the
 * running loop gets its chain, and that is a property of the wiring.
 *
 * <p>⚠️ **WALL CLOCK, NOT AN INJECTED ONE.** M5's and M7's zero-request proofs
 * drive a {@code Clock} seam; the thing this adds is that no REAL timer in the
 * assembled process fires a request. So the loop is counted by its own tick
 * counter rather than assumed to have run -- "no request in a second" is
 * equally true of a timer that never fired.
 *
 * <p>⚠️ **NO DOCKER.** The claim is about which requests are made, and a
 * counting decorator over an in-memory store sees every one of them.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AssembledGcCostIT {

    private static final String INDEX = "logs";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of(INDEX));

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    private static String indexUuid() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    private static ServerConfig config(RetentionConfig retention) {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://pod1:8080", IngestConfig.defaults("cluster-a"),
                0, "producer-1", Set.of(INDEX), retention, java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty());
    }

    private static void awaitTicks(Assembly assembly, long atLeast) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (assembly.retention().ticks() < atLeast) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the retention timer ran only "
                        + assembly.retention().ticks() + " times; wanted " + atLeast);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void TENPassesOnTheREALTimerWithNothingExpiredCostZEROListGetAndDelete()
            throws Exception {
        // ⚠️ THE DEFAULT FLOOR (NFR-13's 6 h) AND A SHORT INTERVAL: the floor
        // is what makes "nothing expired" true of a segment written a moment
        // ago, and the interval is what gets ten real ticks into a test.
        RetentionConfig retention = new RetentionConfig(RetentionConfig.DEFAULT_MIN_RETENTION,
                RetentionConfig.DEFAULT_MAX_RETENTION, RetentionConfig.DEFAULT_REPORT_TIMEOUT,
                RetentionConfig.DEFAULT_COPY_EXPIRY, Duration.ofMillis(20));
        try (MemoryBinStore backing = new MemoryBinStore()) {
            CountingBinStore store = new CountingBinStore(backing);
            try (Assembly assembly = Assembly.open(config(retention), store, noPeers(),
                    Clock.systemUTC())) {
                assembly.catalog().register(
                        new IndexRegistration(indexUuid(), INDEX, List.of(), 4, 4, 1, 1));
                assembly.ingest().append(PRODUCER, INDEX, 0, sink -> sink.accept(
                        new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                                "{}".getBytes(StandardCharsets.UTF_8))));

                // ⚠️ THE PREMISE, OR THE ZERO BELOW IS ABOUT NOTHING: this node
                // leads, and the chain the loop reads holds the commit.
                // ⚠️ UNDER THE BATCHER (M8.50): the held term is wrapped.
                assertThat(LocalSequencer.underneath(assembly.heldTerm()))
                        .as("the write elected a term on this node")
                        .isPresent();
                assertThat(LocalSequencer.underneath(assembly.heldTerm()).orElseThrow()
                        .chain().snapshot().deltas())
                        .as("and its in-memory chain holds the commit the loop will read")
                        .isNotEmpty();

                long from = assembly.retention().ticks();
                long readFrom = assembly.retention().termTicks();
                StoreCounts before = store.counts();
                // ⚠️ AN ELEVENTH TICK STARTED, so the tenth FINISHED: `ticks`
                // counts a tick as it starts and `termTicks` part-way through,
                // and ticks run one at a time (`scheduleWithFixedDelay`).
                // Awaiting the tenth alone raced its own term read -- measured
                // 9 of 10 in two runs of four.
                awaitTicks(assembly, from + 11);
                StoreCounts after = store.counts();

                assertThat(assembly.retention().termTicks() - readFrom)
                        .as("⚠️ AND EVERY ONE OF THOSE TICKS READ THE TERM's CHAIN. Review "
                                + "MEASURED this case green with the root's term source "
                                + "replaced by `Optional.empty()` -- a GC loop that "
                                + "never runs costs nothing, and the zeros below would then be "
                                + "about nothing")
                        .isGreaterThanOrEqualTo(10);

                assertThat(after.lists() - before.lists())
                        .as("⚠️ ZERO LIST. The recovery walk this replaces costs one per 1,000 "
                                + "deltas, per pass, for ever")
                        .isZero();
                assertThat(after.gets() - before.gets())
                        .as("⚠️ ZERO GET. The same walk costs one per DELTA, which grows with "
                                + "the age of the term rather than with the work")
                        .isZero();
                assertThat(after.deletes() - before.deletes())
                        .as("and zero DELETE: nothing is past the 6 h floor")
                        .isZero();
            }
        }
    }

    @Test
    void theLOOPTheRootBuiltDeletesInTheNAMEDBatch() throws Exception {
        // ⚠️ M7.26. Every "1 DELETE per 1,000 keys" assertion in M7 passed
        // `1000` from its own body, and no production call site existed to
        // pass anything else. A wiring that passed 1 compiles and passes
        // every test, at a thousand times the DELETE cost.
        try (MemoryBinStore backing = new MemoryBinStore();
                Assembly assembly = Assembly.open(config(RetentionConfig.defaults()), backing,
                        noPeers(), Clock.systemUTC())) {
            assertThat(assembly.retention().deleteBatch())
                    .isEqualTo(io.github.huyz0.os.biningester.ingest.SegmentGc.DEFAULT_DELETE_BATCH)
                    .isEqualTo(1000);
        }
    }

    @Test
    void aTERMThisNodeStillHoldsButNoLongerSERVESIsNotReadByTheLoop() throws Exception {
        // ⚠️ THE WIRING HALF OF A BLOCKING FINDING. `heldTerm()` keeps
        // returning a term after it is fenced or closed, and a loop fed that
        // term's frozen chain sweeps a successor's hours with it as the keep
        // list. `RetentionLoopTest` pins the loop; this pins that the ROOT
        // hands it `serving()` rather than mere possession.
        RetentionConfig retention = new RetentionConfig(RetentionConfig.DEFAULT_MIN_RETENTION,
                RetentionConfig.DEFAULT_MAX_RETENTION, RetentionConfig.DEFAULT_REPORT_TIMEOUT,
                RetentionConfig.DEFAULT_COPY_EXPIRY, Duration.ofDays(1));
        try (MemoryBinStore backing = new MemoryBinStore();
                Assembly assembly = Assembly.open(config(retention), backing, noPeers(),
                        Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(indexUuid(), INDEX, List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRODUCER, INDEX, 0, sink -> sink.accept(
                    new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                            "{}".getBytes(StandardCharsets.UTF_8))));
            LocalSequencer term = LocalSequencer.underneath(assembly.heldTerm()).orElseThrow();
            assembly.retention().tick();
            assertThat(assembly.retention().termTicks())
                    .as("the premise: a serving term IS read").isEqualTo(1);

            // ⚠️ CLOSED STANDS IN FOR FENCED: both make `serving()` false, and
            // a test cannot stage a second node's takeover in-process. The
            // node still holds the object -- which is the whole point.
            term.close();
            assertThat(LocalSequencer.underneath(assembly.heldTerm())).as("still HELD")
                    .containsSame(term);
            assembly.retention().tick();

            assertThat(assembly.retention().termTicks())
                    .as("⚠️ AND NOT READ: a term that no longer serves is no keep list")
                    .isEqualTo(1);
        }
    }
}
