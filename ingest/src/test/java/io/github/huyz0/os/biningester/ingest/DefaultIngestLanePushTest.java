// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Push follows lanes (M10 criterion 10; ADR-0074 decision 5), T1 over the real
 * ingest, hub and serving path.
 *
 * <p>⚠️ THE LANES ARE CHOSEN AGAINST KEY ORDER: partition 0 carries lane -1,
 * partition 1 lane 0, partition 2 lane +2, so the directory's key order is
 * exactly the reverse of the lane order and a push in key order fails here.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestLanePushTest {

    private static RunKey key(int partition) {
        return new RunKey(IngestTestSupport.LOGS, partition);
    }

    /** Records, on one shared ordered log, which runs each open was handed. */
    private record Logging(List<List<Integer>> log) implements SubscriptionHub.Subscriber {
        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            List<Integer> partitions = new ArrayList<>();
            pushes.forEach(p -> partitions.add(p.key().partitionId()));
            log.add(partitions);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            return (buffer, offset, length) -> out.write(buffer, offset, length);
        }
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, int partition,
            byte lane) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", partition, lane,
                        IngestTestSupport.docs(2)::forEach);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /** Writes lane -1 to p0, lane 0 to p1 and lane +2 to p2, all in ONE segment. */
    private static void writeOneMixedLaneSegment(SubscriptionHub hub, List<?> log, int opens)
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        long dataPuts;
        try (DefaultIngest ingest = IngestTestSupport.ingest(store, hub, IngestTestSupport.NEVER)) {
            long base = store.putPurposeCounts().dataPuts();
            List<CompletableFuture<AppendResult>> appends = List.of(
                    append(ingest, 0, (byte) -1), append(ingest, 1, (byte) 0),
                    append(ingest, 2, (byte) 2));
            IngestTestSupport.awaitPending(ingest, 3);
            ingest.flushNow();
            for (CompletableFuture<AppendResult> a : appends) {
                a.get(10, TimeUnit.SECONDS);
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (log.size() < opens && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            dataPuts = store.putPurposeCounts().dataPuts() - base;
        }
        assertThat(dataPuts)
                .as("PREMISE: one data PUT, so the three runs share one segment")
                .isOne();
    }

    @Test
    void aSEGMENTsPushReachesThePOSITIVERunBeforeLane0BeforeTheNEGATIVE() throws Exception {
        // ⚠️ ONE SUBSCRIBER PER RUN, which is how every production transport
        // registers (M5.40a: "K is always 1"), so the ORDER OF OPENS is the
        // order in which the segment's runs reach their consumers.
        SubscriptionHub hub = new SubscriptionHub();
        List<List<Integer>> log = Collections.synchronizedList(new ArrayList<>());
        try (var s0 = hub.subscribe(key(0), new Logging(log));
                var s1 = hub.subscribe(key(1), new Logging(log));
                var s2 = hub.subscribe(key(2), new Logging(log))) {
            writeOneMixedLaneSegment(hub, log, 3);
        }
        assertThat(log)
                .as("+2 (p2), then 0 (p1), then -1 (p0) -- never key order p0, p1, p2")
                .containsExactly(List.of(2), List.of(1), List.of(0));
    }

    @Test
    void ONEConsumerHoldingEveryRunIsHandedThemHIGHESTLaneFirst() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        List<List<Integer>> log = Collections.synchronizedList(new ArrayList<>());
        Logging one = new Logging(log);
        try (var s0 = hub.subscribe(key(0), one);
                var s1 = hub.subscribe(key(1), one);
                var s2 = hub.subscribe(key(2), one)) {
            writeOneMixedLaneSegment(hub, log, 1);
        }
        assertThat(log).as("one open, its runs in lane order")
                .containsExactly(List.of(2, 1, 0));
    }
}
