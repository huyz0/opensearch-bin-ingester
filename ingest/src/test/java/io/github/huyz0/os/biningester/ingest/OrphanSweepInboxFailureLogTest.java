// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.sequencer.CommitLog;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * M12.19a (M10.27 T3): the orphan sweep names the cost governor ONLY when the
 * governor refused its inbox read. A plain store failure on the same read must
 * not be reported as a governor refusal -- that would send an operator to the
 * LIST ceiling for what is an outage.
 */
class OrphanSweepInboxFailureLogTest {

    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);

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
    private final java.util.concurrent.atomic.AtomicInteger inboxReads =
            new java.util.concurrent.atomic.AtomicInteger();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    /** {@code backing}, whose inbox LIST fails as an outage would: a plain IOException. */
    private BinStore inboxUnreadable() {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("list") && ((String) args[0]).contains("/inbox/")) {
                        inboxReads.incrementAndGet();
                        throw new IOException("connection reset reading the inbox");
                    }
                    try {
                        return method.invoke(backing, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    void aPlainFailureOnTheInboxReadIsNotReportedAsAGovernorRefusal() throws Exception {
        CommitLog log = new CommitLog(backing, PREFIX + "/ctl/log", 1);
        RetentionRule rule = new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, 0));
        RetentionObservable observable = new RetentionObservable(clock, MIN, MAX,
                Duration.ofMinutes(1), alarm -> { });
        RetentionLoop loop = new RetentionLoop(
                () -> Optional.of(new RetentionLoop.Term(log.chain(), boundary -> { }, () -> true)),
                new LeasedGc(new FreeLease(), inboxUnreadable()), rule, observable, clock, PREFIX,
                MIN, OrphanSweep.DEFAULT_GRACE, SegmentGc.DEFAULT_DELETE_BATCH, () -> true);
        loop.tick(); // the term is first seen at 10:30, so the sweep may start at 12:00
        String orphan = new SegmentKey(PREFIX,
                Instant.parse("2026-09-17T12:10:00Z").toEpochMilli(), "poda", 1, 12).key();
        backing.put(orphan, Body.ofBytes(new byte[] {1}));

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        int readsBefore = inboxReads.get();
        String logged = LogCapture.capturing(loop::tick);

        assertThat(inboxReads.get() - readsBefore)
                .as("the premise: this tick's sweep reached its inbox read, and it failed")
                .isPositive();
        assertThat(backing.stat(orphan)).as("the premise: the unread inbox stopped the sweep")
                .isPresent();
        assertThat(logged)
                .as("⚠️ AN OUTAGE IS NOT A GOVERNOR REFUSAL, and the log must not say it is")
                .doesNotContain("refused by the cost governor");
    }
}
