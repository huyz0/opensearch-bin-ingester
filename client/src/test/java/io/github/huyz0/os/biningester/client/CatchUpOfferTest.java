// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;

class CatchUpOfferTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ae"), 0);

    @Test
    void fullLaneRejectsWithoutWaitingAndAcceptsAfterTheConsumerMakesRoom() throws Exception {
        try (ConsumerClient client = new ConsumerClient(KEY, 1, null)) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);

            assertThat(client.tryDeliverCatchUp(request, delivery(0, 2))).isTrue();
            boolean acceptedWhileFull = client.tryDeliverCatchUp(request, delivery(2, 2));
            assertThat(acceptedWhileFull).isFalse();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.tryDeliverCatchUp(request, delivery(2, 2))).isTrue();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(3);
            assertThat(client.completeCatchUp(request)).isTrue();
        }
    }

    @Test
    void refusingAFullLaneDoesNotLeaveItsPrivateAvailabilityPermit() throws Exception {
        UUID request = UUID.randomUUID();
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(1, new Object(),
                new java.util.concurrent.Semaphore(0));
        Delivery first = delivery(2, 2);
        Delivery second = delivery(4, 2);
        lane.begin(request);

        assertThat(lane.tryPut(request, first)).isTrue();
        assertThat(lane.tryPut(request, second)).isFalse();
        assertThat(lane.tryAcquireDelivery()).isTrue();
        assertThat(lane.poll()).isSameAs(first);
        assertThat(lane.tryAcquireDelivery())
                .as("a refused delivery contributes no catch-up-only semaphore permit")
                .isFalse();

        assertThat(lane.tryPut(request, second)).isTrue();
        assertThat(lane.tryAcquireDelivery()).isTrue();
        assertThat(lane.poll()).isSameAs(second);
        assertThat(lane.tryAcquireDelivery()).isFalse();
    }

    /**
     * ⚠️ THE SHARED PERMIT, NOT ONLY THE LANE's OWN (M10.11, harvested from
     * bf8877b's M9.39-T2 and c1d9f54's M9.40-T2). The case above proves a
     * refusal leaves no catch-up-only permit; the node-wide
     * {@code deliveryAvailable} that live deliveries also signal is a second
     * semaphore, and a permit left there by a refusal is a wake-up for a
     * delivery that does not exist.
     */
    @Test
    void refusingAFullLaneLeavesNoPermitOnTheSharedDeliverySignal() throws Exception {
        UUID request = UUID.randomUUID();
        Semaphore shared = new Semaphore(0);
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(1, new Object(), shared);
        lane.begin(request);

        assertThat(lane.tryPut(request, delivery(2, 2))).isTrue();
        assertThat(shared.availablePermits()).as("one accepted delivery, one signal").isOne();
        assertThat(lane.tryPut(request, delivery(4, 2))).isFalse();
        assertThat(shared.availablePermits()).as("the refusal signals nothing").isOne();
    }

    private static Delivery delivery(long offset, int recordCount) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (int i = 0; i < recordCount; i++) {
            long recordOffset = offset + i;
            writer.add(KEY, new SegmentRecord("doc-" + recordOffset, OpType.INDEX,
                    OptionalLong.of(recordOffset), "{}".getBytes(StandardCharsets.UTF_8)), recordOffset);
        }
        return new Delivery(KEY, "segment-" + offset, recordCount, offset, FetchMode.INLINE,
                writer.toByteArray(11L));
    }
}
