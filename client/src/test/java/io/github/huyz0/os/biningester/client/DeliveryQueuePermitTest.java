// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A delivery whose decode THROWS gives back the permits it took, and stays
 * queued (M10.23, the M10.2 review's finding R3).
 *
 * <p>⚠️ BEFORE THIS, A THROW LEAKED THEM: the delivery stayed at the head but
 * nothing could reach it, so the next poll was an ordinary empty poll over a
 * queue that was not empty -- and on the replay lane the delivery had already
 * been taken off the queue, so it was simply gone.
 */
class DeliveryQueuePermitTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ce"), 0);

    /** Throws on its first decode and decodes one record per delivery after. */
    private static final class ThrowsOnce implements ConsumerDeliveryQueues.Decoder {
        final AtomicInteger calls = new AtomicInteger();
        final List<Delivery> seen = new CopyOnWriteArrayList<>();

        @Override public boolean decode(Delivery delivery,
                java.util.Deque<ConsumerRecord> out, boolean replay) {
            seen.add(delivery);
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("the decode failed");
            }
            out.add(new ConsumerRecord(delivery.firstOffset(), new SegmentRecord("r",
                    OpType.INDEX, OptionalLong.of(1), "{}".getBytes(StandardCharsets.UTF_8)), 1L));
            return true;
        }
    }

    private static Delivery delivery(long offset) {
        return new Delivery(KEY, "seg", 1, offset, FetchMode.INLINE, new byte[] {1});
    }

    @Test
    void aLiveDeliveryWhoseDecodeThrowsIsReadOnTheNextPoll() throws Exception {
        ThrowsOnce decoder = new ThrowsOnce();
        ConsumerDeliveryQueues queues = new ConsumerDeliveryQueues(4, decoder, () -> false, () -> false);
        Delivery d = delivery(9);
        assertThat(queues.deliverLive(d)).isTrue();

        assertThatThrownBy(() -> queues.readNext(Duration.ZERO))
                .isInstanceOf(IllegalStateException.class);
        assertThat(queues.queuedLiveDeliveries()).as("still queued").isEqualTo(1);

        assertThat(queues.readNext(Duration.ZERO)).map(ConsumerRecord::offset).contains(9L);
        assertThat(decoder.seen).containsExactly(d, d);
        assertThat(queues.queuedLiveDeliveries()).isZero();
        assertThat(queues.readNext(Duration.ZERO))
                .as("and read once: no permit was minted twice").isEmpty();
    }

    @Test
    void aReplayDeliveryWhoseDecodeThrowsIsNotLost() throws Exception {
        ThrowsOnce decoder = new ThrowsOnce();
        ConsumerDeliveryQueues queues = new ConsumerDeliveryQueues(4, decoder, () -> false, () -> false);
        UUID request = UUID.fromString("00000000-0000-0000-0000-00000000c0de");
        queues.beginCatchUp(request, false);
        Delivery d = delivery(3);
        queues.deliverCatchUp(request, d);

        assertThatThrownBy(() -> queues.readNext(Duration.ZERO))
                .isInstanceOf(IllegalStateException.class);

        assertThat(queues.readNext(Duration.ZERO)).map(ConsumerRecord::offset).contains(3L);
        assertThat(decoder.seen).containsExactly(d, d);
        assertThat(queues.completeCatchUp(request))
                .as("its one record was handed out, so the exchange completes").isTrue();
        assertThat(queues.readNext(Duration.ZERO)).isEmpty();
    }
}
