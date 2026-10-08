// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A void reaches a consumer as a counted skip, not an upstream gap (M13.25e,
 * ADR-0082 §5's last bullet).
 *
 * <p>⚠️ **A VOID IS A COMMITTED HOLE**, so the records after it are the next
 * ones in order: read as an offset jump, they were reported as records lost
 * upstream, and a consumer with a repair handler held the stream for a repair
 * no replay could complete.
 */
class DeliveryVoidTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000ab"), 0);

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        return new Delivery(KEY, "seg-" + firstOffset, ids.length, firstOffset,
                FetchMode.INLINE, segmentOf(ids));
    }

    private static List<Long> drain(ConsumerClient client) throws Exception {
        List<Long> offsets = new ArrayList<>();
        Optional<io.github.huyz0.os.biningester.client.ConsumerRecord> r;
        while ((r = client.readNext(Duration.ofMillis(50))).isPresent()) {
            offsets.add(r.get().offset());
        }
        return offsets;
    }

    @Test
    void aVOIDIsSkippedAndCountedNotReportedAsAGap() throws Exception {
        ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch());
        client.deliver(delivery(0, "a", "b"));
        client.deliver(Delivery.voidRange(KEY, 2, 3, 7L, 11L));
        client.deliver(delivery(5, "c"));

        assertThat(drain(client)).as("records on both sides, none for the hole")
                .containsExactly(0L, 1L, 5L);
        assertThat(client.gapsDetected()).as("⚠️ A HOLE THE CHAIN COMMITTED IS NO GAP").isZero();
        assertThat(client.voidedOffsetsSkipped()).isEqualTo(3);
    }

    @Test
    void aVOIDPastMissingRecordsIsStillAGap() throws Exception {
        ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch());
        client.deliver(delivery(0, "a"));
        client.deliver(Delivery.voidRange(KEY, 4, 2, 7L, 11L));

        drain(client);

        assertThat(client.gapsDetected()).as("offsets 1-3 are neither records nor void")
                .isEqualTo(1);
    }

    @Test
    void aVOIDHeldBehindAGapIsCountedOnceWhenItCommits() throws Exception {
        // ⚠️ M13.25e review T2: the void ITSELF exposes the gap, so its first
        // commit decodes nothing and is held; counted on any commit rather than
        // on the one that decodes it, it was counted at the gap and again after.
        ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch());
        java.util.List<DeliveryGapException> gaps = new java.util.ArrayList<>();
        client.onGap(gaps::add);
        client.deliver(delivery(0, "a"));
        client.deliver(Delivery.voidRange(KEY, 2, 2, 7L, 11L));
        drain(client);
        drain(client);
        assertThat(gaps).as("the premise: offset 1 is missing").hasSize(1);

        java.util.UUID replay = java.util.UUID.randomUUID();
        client.beginCatchUp(replay);
        client.deliverCatchUp(replay, delivery(1, "b"));
        client.completeCatchUp(replay);
        client.completeGapRepair();
        drain(client);

        assertThat(client.voidedOffsetsSkipped()).as("counted once").isEqualTo(2);
    }

    @Test
    void aVOIDReplayedAfterTheLiveLaneCommittedItIsNotCountedAgain() throws Exception {
        // ⚠️ M13.25e review P2: a replay re-delivers what the live lane already
        // committed; only the offsets a commit newly covers are counted.
        ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch());
        client.deliver(delivery(0, "a"));
        client.deliver(Delivery.voidRange(KEY, 1, 3, 7L, 11L));
        drain(client);

        java.util.UUID replay = java.util.UUID.randomUUID();
        client.beginCatchUp(replay);
        client.deliverCatchUp(replay, Delivery.voidRange(KEY, 1, 3, 7L, 11L));
        client.completeCatchUp(replay);
        drain(client);

        assertThat(client.voidedOffsetsSkipped()).isEqualTo(3);
        assertThat(client.catchUpComplete(replay))
                .as("⚠️ M13.25e review P1: AN EXCHANGE ENDING IN A VOID COMPLETES").isTrue();
    }

    @Test
    void aVOIDCarryingASegmentOrBytesIsRefused() {
        // ⚠️ M13.25e review T3.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new Delivery(KEY, "seg", 3, 1,
                FetchMode.INLINE, new byte[0], null, 7L, 11L, true))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new Delivery(KEY, "", 3, 1,
                FetchMode.INLINE, new byte[] {1}, null, 7L, 11L, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theLIVEDispatchCarriesTheVoid() {
        Delivery delivery = HttpSubscriptionTransport.deliveryFor(
                SubscriptionEvent.voidRange("s", 7L, 3L, KEY, 2, 3, 11L));

        assertThat(delivery.voided()).isTrue();
        assertThat(delivery.firstOffset()).isEqualTo(2);
        assertThat(delivery.recordCount()).isEqualTo(3);
    }
}
