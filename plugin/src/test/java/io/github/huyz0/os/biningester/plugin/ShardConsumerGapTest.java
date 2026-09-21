// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IngestionShardConsumer.ReadResult;

/**
 * A gap reaches the SHARD, and the poll keeps working (M6.1, M6 criterion 7).
 *
 * <p>⚠️ THE CLIENT-SIDE CASES CANNOT SEE THIS. {@code DeliveryGapTest} asserts
 * at {@link ConsumerClient}, and round-1 review named the consequence: an
 * earlier draft of this task THREW from {@code readNext}, every client case
 * passed, and the defect was two frames up — {@code drain} accumulates records
 * into a list and a throw would have discarded the ones already polled, losing
 * MORE records than the gap did. Research doc 02 § 6 rules the throw out
 * outright: an exception from {@code readNext} pauses the shard until an
 * operator resumes it.
 *
 * <p>⚠️ SO WHAT IS ASSERTED HERE IS THE PLUGIN-LEVEL BEHAVIOUR: the records
 * that DID arrive are handed over, the poll goes on working, and the loss is
 * visible as a number rather than as silence.
 */
class ShardConsumerGapTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(INDEX, 0);

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

    private static List<Long> offsetsOf(
            List<ReadResult<BinStoreOffset, BinStoreMessage>> results) {
        List<Long> out = new ArrayList<>();
        results.forEach(r -> out.add(r.getPointer().offset()));
        return out;
    }

    @Test
    void aGapDoesNotSTOPTheShardAndDoesNotLOSETheRecordsAroundIt() throws Exception {
        ConsumerClient client = new ConsumerClient(KEY, 16, null);
        BinStoreShardConsumer shard = new BinStoreShardConsumer(0, client);
        client.deliver(delivery(0, "a", "b"));
        client.deliver(delivery(9, "j"));

        List<ReadResult<BinStoreOffset, BinStoreMessage>> read = shard.readNext(10, 200);

        assertThat(offsetsOf(read))
                .as("every record that DID arrive is handed over -- an exception from readNext "
                        + "would have discarded the ones already polled into this list, losing "
                        + "more than the gap did, and research doc 02 § 6 says it also pauses "
                        + "the shard until an operator resumes it")
                .containsExactly(0L, 1L, 9L);
        assertThat(shard.gapsDetected())
                .as("and the loss is a NUMBER an operator can see rather than silence: offsets "
                        + "2 through 8 were never delivered")
                .isEqualTo(1);
    }

    @Test
    void aSHARDWithNoGapReportsNONE() throws Exception {
        ConsumerClient client = new ConsumerClient(KEY, 16, null);
        BinStoreShardConsumer shard = new BinStoreShardConsumer(0, client);
        client.deliver(delivery(0, "a", "b"));
        client.deliver(delivery(2, "c"));

        List<ReadResult<BinStoreOffset, BinStoreMessage>> read = shard.readNext(10, 200);

        assertThat(offsetsOf(read)).containsExactly(0L, 1L, 2L);
        assertThat(shard.gapsDetected())
                .as("a counter that reported a gap on a healthy stream would page an operator "
                        + "for every shard in the cluster")
                .isZero();
    }
}
