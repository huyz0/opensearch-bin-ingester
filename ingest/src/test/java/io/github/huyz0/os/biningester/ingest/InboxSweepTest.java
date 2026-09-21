// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The orphan sweep keeps a segment an inbox intent names (M8.14a, ADR-0058).
 *
 * <p>⚠️ **AN INTENT's SEGMENT IS ACKED DATA** whose delta the drain has not
 * written yet: in no delta, so it looks exactly like an orphan, and review
 * MEASURED the sweep taking it an hour after a partition began -- records the
 * producer was told were durable.
 */
class InboxSweepTest {

    private static final RunKey STREAM = new RunKey(new UUID(0x5151_5151L, 1), 0);
    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);
    private static final Duration GRACE = OrphanSweep.DEFAULT_GRACE;

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void set(Instant at) {
            now = at;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static final class FreeLease implements GcLease {
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
    }

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    private RetentionLoop loop(CommitLog log) {
        return loop(log, store);
    }

    private RetentionLoop loop(CommitLog log, io.github.huyz0.os.biningester.binstore.BinStore gcStore) {
        RetentionRule nothingDue = new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, 0));
        RetentionObservable observable = new RetentionObservable(clock, MIN, MAX,
                Duration.ofMinutes(1), alarm -> { });
        return new RetentionLoop(
                () -> Optional.of(new RetentionLoop.Term(log.chain(), retained -> { },
                        () -> true)),
                new LeasedGc(new FreeLease(), gcStore), nothingDue, observable, clock, PREFIX,
                MIN, GRACE, SegmentGc.DEFAULT_DELETE_BATCH);
    }

    private String put(Instant writtenAt, long sequence) throws Exception {
        String key = new SegmentKey(PREFIX, writtenAt.toEpochMilli(), "poda", sequence, 12).key();
        backing.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    @Test
    void aSEGMENTAnINTENTNamesIsKEPTWhileAnOrphanBesideItGOES() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        RetentionLoop loop = loop(log);
        loop.tick(); // the term is first seen at 10:30, so the sweep may start at 12:00

        String orphan = put(Instant.parse("2026-09-17T12:10:00Z"), 1);
        String deferred = put(Instant.parse("2026-09-17T12:20:00Z"), 2);
        Inbox.write(backing, PREFIX, new CommitRequest("podb", "i1", 4, deferred,
                Map.of(STREAM, 3)));

        clock.set(Instant.parse("2026-09-17T13:00:01Z").plus(GRACE));
        loop.tick();

        assertThat(backing.stat(orphan)).as("the premise: the hour was swept").isEmpty();
        assertThat(backing.stat(deferred))
                .as("⚠️ NAMED BY AN INTENT: acked, awaiting its drain, never an orphan")
                .isPresent();
    }

    @Test
    void aDRAINBetweenTheSnapshotAndTheInboxReadLOSESNothing() throws Exception {
        // ⚠️ REVIEW R1: the tick's snapshot predates the drain, the drain then
        // commits the intent and DELETES it, and the inbox read finds nothing --
        // so the segment was in neither set. The chain is read again after the
        // inbox, and a drain writes the delta before it deletes the intent.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        String deferred = put(Instant.parse("2026-09-17T12:20:00Z"), 2);
        CommitRequest intent = new CommitRequest("podb", "i1", 4, deferred, Map.of(STREAM, 3));
        Inbox.write(backing, PREFIX, intent);
        AtomicBoolean drained = new AtomicBoolean();
        io.github.huyz0.os.biningester.binstore.BinStore drainsOnTheInboxList = (io.github.huyz0.os.biningester.binstore.BinStore)
                java.lang.reflect.Proxy.newProxyInstance(
                        io.github.huyz0.os.biningester.binstore.BinStore.class.getClassLoader(),
                        new Class<?>[] {io.github.huyz0.os.biningester.binstore.BinStore.class}, (proxy, m, args) -> {
                            if (m.getName().equals("list")
                                    && String.valueOf(args[0]).equals(Inbox.prefixFor(PREFIX))
                                    && drained.compareAndSet(false, true)) {
                                log.commit(deferred, Map.of(STREAM, 3));
                                backing.delete(List.of(Inbox.keyFor(PREFIX, intent)));
                            }
                            try {
                                return m.invoke(store, args);
                            } catch (java.lang.reflect.InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
        RetentionLoop loop = loop(log, drainsOnTheInboxList);
        loop.tick();

        clock.set(Instant.parse("2026-09-17T13:00:01Z").plus(GRACE));
        loop.tick();

        assertThat(drained.get()).as("the premise: the drain ran mid-tick").isTrue();
        assertThat(backing.stat(deferred))
                .as("⚠️ COMMITTED BY THE DRAIN MID-TICK: kept, not swept as an orphan")
                .isPresent();
    }
}
