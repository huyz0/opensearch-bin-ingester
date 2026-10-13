// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.INDEX;
import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SegmentFetchRetry;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Each re-published offset delivered exactly once across a takeover (M13.33t;
 * SPEC test-plan row 12, ADR-0081 §5 step 6, ADR-0085): a subscriber on the
 * pod that takes over was pushed two of a dead term's entries by PUBLISH,
 * under their fast keys; the new leader recovers those two and a third it
 * never pushed, and pushes the recovered segment live (M13.33w). The
 * subscriber's consumer reads each of the three offsets once -- the overlap
 * trimmed, the third not missing.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverSubscriberTest {

    private static Delivery delivery(SubscriptionHub.Push p) {
        return p.voided()
                ? Delivery.voidRange(p.key(), p.firstOffset(), p.recordCount(),
                        p.sequencerEpoch(), p.chainSequence())
                : new Delivery(p.key(), p.segmentKey(), p.recordCount(), p.firstOffset(),
                        p.via(), p.segment(), p.grant(), p.sequencerEpoch(), p.chainSequence());
    }

    @Test
    void eachREPUBLISHEDOffsetIsReadOnceAcrossTheTakeover() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5,
                Map.of("uid-m", List.of(entry(1, 5), entry(1, 6), entry(1, 7))))) {
            SubscriptionHub hub = new SubscriptionHub();
            ConsumerClient client = new ConsumerClient(S, 64, null,
                    SegmentFetchRetry.standard(() -> 0L));
            List<String> pushedKeys = new java.util.concurrent.CopyOnWriteArrayList<>();
            Duration never = Duration.ofHours(1);
            try (var sub = hub.subscribe(S, SubscriptionHub.assembling(p -> {
                        pushedKeys.add(p.segmentKey());
                        client.deliver(delivery(p));
                    }));
                    DefaultIngest ingest = new DefaultIngest(new IngestConfig(never, 8L << 20,
                            "cluster-a", IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, never,
                            IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                            IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD,
                            IngestConfig.DEFAULT_INTERVAL_LENGTHEN_DELAY,
                            IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY), rig.store, "p", "self",
                            rig.term, hub, TakeoverRig.WALL, index -> INDEX, key -> { },
                            new IndexCostLedger())) {
                // the dead term's PUBLISH reached this pod for two of the three
                ingest.deliverPushed(entry(1, 5));
                ingest.deliverPushed(entry(1, 6));

                assertThat(rig.takeOver(ingest::publishRecovery, ingest::recovered)).isTrue();

                List<Long> read = new ArrayList<>();
                List<String> ids = new ArrayList<>();
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (read.size() < 3 && System.nanoTime() < deadline) {
                    Optional<ConsumerRecord> r = client.readNext(Duration.ofMillis(50));
                    r.ifPresent(record -> {
                        read.add(record.offset());
                        ids.add(record.record().id());
                    });
                }
                // anything further would be a second delivery of an offset
                Optional<ConsumerRecord> extra = client.readNext(Duration.ofMillis(300));

                assertThat(read).as("each offset once, in order").containsExactly(5L, 6L, 7L);
                assertThat(ids).containsExactly("d1-5", "d1-6", "d1-7");
                assertThat(extra).as("nothing read twice").isEmpty();
                assertThat(pushedKeys).as("two fast pushes, then the recovered segment")
                        .hasSize(3).last().asString().doesNotContain("/fast/");
                assertThat(client.gapsDetected()).isZero();
            }
        }
    }
}
