// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The retention loop hands chain GC the FENCED store, after the segment pass,
 * and never on the idle path (M8.39).
 */
class RetentionLoopChainGcTest {

    private static final RunKey STREAM = new RunKey(new UUID(0x5151_5151L, 2), 0);
    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);
    private static final Instant NOW = Instant.parse("2026-09-17T10:30:00Z");

    private final MemoryBinStore store = new MemoryBinStore();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    private final List<BinStore> handed = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        store.close();
    }

    private RetentionLoop loop(CommitLog log) {
        return loop(log, () -> true);
    }

    private RetentionLoop loop(CommitLog log, java.util.function.BooleanSupplier live) {
        RetentionRule readToTheEnd = new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, Long.MAX_VALUE / 2));
        GcLease free = new GcLease() {
            @Override
            public boolean acquire() {
                return true;
            }

            @Override
            public boolean stillHeld() {
                return true;
            }

            @Override
            public void release() {
            }
        };
        return new RetentionLoop(
                () -> Optional.of(new RetentionLoop.Term(log.chain(), offsets -> { },
                        live, handed::add)),
                new LeasedGc(free, store), readToTheEnd,
                new RetentionObservable(clock, MIN, MAX, Duration.ofMinutes(1), alarm -> { }),
                clock, PREFIX, MIN, OrphanSweep.DEFAULT_GRACE, SegmentGc.DEFAULT_DELETE_BATCH);
    }

    private void committed(CommitLog log, Instant writtenAt) throws Exception {
        String key = new SegmentKey(PREFIX, writtenAt.toEpochMilli(), "poda", 1, 12).key();
        store.put(key, Body.ofBytes(new byte[] {1}));
        log.commit(key, Map.of(STREAM, 5));
    }

    @Test
    void aPASSThatRanHandsChainGcTheFENCEDStore() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        committed(log, NOW.minus(MIN).minus(Duration.ofHours(1)));

        loop(log).tick();

        assertThat(handed).as("chain GC ran once, after the segment pass").hasSize(1);
        assertThat(handed.get(0))
                .as("⚠️ THROUGH THE GC LEASE's STORE, not the raw one: a node that lost "
                        + "the lease must not delete")
                .isNotSameAs(store);
    }

    @Test
    void anIDLETickDoesNotRunChainGc() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        committed(log, NOW.minus(Duration.ofMinutes(1)));

        loop(log).tick();

        assertThat(handed).as("⚠️ NOTHING DUE, SO NOTHING RUNS: an idle leader issues "
                + "nothing for chain GC either").isEmpty();
    }

    @Test
    void aTermDEPOSEDAfterTheSegmentPassDoesNotRunChainGc() throws Exception {
        // ⚠️ ASKED AGAIN BEFORE CHAIN GC: a term fenced between the two passes
        // holds a chain frozen at the takeover. The first two asks (the tick's
        // own and the segment pass's) say live; the third does not.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        committed(log, NOW.minus(MIN).minus(Duration.ofHours(1)));
        java.util.concurrent.atomic.AtomicInteger asks = new java.util.concurrent.atomic.AtomicInteger();

        loop(log, () -> asks.incrementAndGet() <= 2).tick();

        assertThat(asks).as("the premise: it was asked a third time").hasValueGreaterThan(2);
        assertThat(handed).as("⚠️ DEPOSED, SO CHAIN GC DID NOT RUN").isEmpty();
    }
}
