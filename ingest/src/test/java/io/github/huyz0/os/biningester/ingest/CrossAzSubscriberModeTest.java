// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M10.33, ADR-0076: the fetch mode is chosen PER SUBSCRIBER by zone, not once
 * per segment.
 *
 * <p>⚠️ THE SEGMENT IS SUB-CAP AND ABOVE THE CROSSOVER ON PURPOSE: 100,000
 * bytes is under the 256 KiB inline cap and past research's ~19.5 KiB cross-AZ
 * crossover, which is the size M10's review (F1) found inlined across the zone,
 * payload and all, to a consumer in another AZ.
 */
class CrossAzSubscriberModeTest {

    private static final UUID INDEX = UUID.fromString("7a3c9e10-2222-4333-8444-555566667777");

    /** A subscriber in a declared zone that keeps what it was told and handed. */
    private static final class Zoned implements SubscriptionHub.Subscriber {
        private final String az;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final List<SubscriptionHub.Push> completed = new ArrayList<>();

        Zoned(String az) {
            this.az = az;
        }

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            return (buffer, offset, length) -> bytes.write(buffer, offset, length);
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            completed.addAll(pushes);
        }

        @Override
        public String az() {
            return az;
        }
    }

    @Test
    void aSubCapSegmentIsInlinedWithinTheZoneAndProxiedWithoutBytesAcrossIt() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = new byte[100_000];
        for (int i = 0; i < segment.length; i++) {
            segment[i] = (byte) (i * 31 + 7);
        }
        SubscriptionHub hub = new SubscriptionHub("az-a");
        RunKey key = new RunKey(INDEX, 0);
        Zoned sameZone = new Zoned("az-a");
        Zoned otherZone = new Zoned("az-b");
        Zoned noZone = new Zoned(null);
        Zoned blankZone = new Zoned(" ");
        List<AutoCloseable> handles = new ArrayList<>();
        for (Zoned subscriber : List.of(sameZone, otherZone, noZone, blankZone)) {
            handles.add(hub.subscribe(key, subscriber));
        }

        hub.publish(new CommitDelta(1, "seg-sub-cap", List.of(new RunCommit(key, 3, 10))),
                "seg-sub-cap", segment, new SegmentServing(
                        new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                        store.capabilities(), new SegmentProxy(store)));
        for (AutoCloseable handle : handles) {
            handle.close();
        }

        assertThat(sameZone.completed).extracting(SubscriptionHub.Push::via)
                .as("a subscriber in the pod's own zone keeps the size policy: inline")
                .containsExactly(FetchMode.INLINE);
        assertThat(sameZone.bytes.toByteArray()).isEqualTo(segment);
        assertThat(otherZone.completed).extracting(SubscriptionHub.Push::via)
                .as("a subscriber in ANOTHER zone is told proxy, never inline")
                .containsExactly(FetchMode.PROXY);
        assertThat(otherZone.bytes.size())
                .as("and is handed no payload: its consumer fetches from its own zone")
                .isZero();
        assertThat(noZone.completed).extracting(SubscriptionHub.Push::via)
                .as("a subscriber that declared no zone keeps the size policy: inline")
                .containsExactly(FetchMode.INLINE);
        assertThat(noZone.bytes.toByteArray()).isEqualTo(segment);
        assertThat(blankZone.completed).extracting(SubscriptionHub.Push::via)
                .as("a blank zone is no zone")
                .containsExactly(FetchMode.INLINE);
        assertThat(store.counts().total())
                .as("choosing the mode per subscriber buys no store request")
                .isZero();
    }

    /**
     * ⚠️ A PADDED {@code pod.az} IS STILL THIS ZONE. The hub trims its own
     * label as it trims a subscriber's; without that, {@code " az-a"} would
     * call every az-a consumer remote and withhold its payload.
     */
    @Test
    void aPaddedPodZoneStillServesItsOwnZoneInline() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = new byte[100_000];
        SubscriptionHub hub = new SubscriptionHub(" az-a ");
        RunKey key = new RunKey(INDEX, 1);
        Zoned sameZone = new Zoned("az-a");
        try (AutoCloseable handle = hub.subscribe(key, sameZone)) {
            hub.publish(new CommitDelta(1, "seg-padded", List.of(new RunCommit(key, 3, 10))),
                    "seg-padded", segment, new SegmentServing(
                            new FetchPolicy(FetchPolicyConfig.defaultsFor(
                                    store.capabilities().costs())),
                            store.capabilities(), new SegmentProxy(store)));
        }

        assertThat(sameZone.completed).extracting(SubscriptionHub.Push::via)
                .as("\" az-a \" and \"az-a\" are one zone")
                .containsExactly(FetchMode.INLINE);
        assertThat(sameZone.bytes.toByteArray()).isEqualTo(segment);
    }
}
