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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A delivery overlapping the next expected offset is trimmed, not refused
 * (ADR-0085; M13.30h): a fast stream's committed run arrives over the entries
 * pushed to the consumer before its upload, so its records below the expected
 * offset are handed on already -- dropped as duplicates, the rest handed on in
 * order, and no exception out of {@code readNext}, which would pause the shard.
 */
class DeliveryOverlapTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000ab");
    private static final RunKey KEY = new RunKey(INDEX, 0);

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return new Delivery(KEY, "seg-" + firstOffset, ids.length, firstOffset,
                FetchMode.INLINE, w.toByteArray(1L));
    }

    private static List<String> drain(ConsumerClient client, int n) throws Exception {
        List<String> read = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Optional<ConsumerRecord> r = client.readNext(Duration.ofMillis(50));
            if (r.isEmpty()) {
                return read;
            }
            read.add(r.get().offset() + ":" + r.get().record().id());
        }
        return read;
    }

    @Test
    void aCOMMITTEDRunOverItsPushedEntriesHandsOnOnlyWhatIsNew() throws Exception {
        ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch());
        client.deliver(delivery(10, "a", "b"));
        client.deliver(delivery(12, "c"));
        assertThat(drain(client, 3)).as("PREMISE: the pushed entries")
                .containsExactly("10:a", "11:b", "12:c");

        client.deliver(delivery(10, "a", "b", "c", "d", "e"));
        client.deliver(delivery(15, "f"));

        assertThat(drain(client, 4)).as("the run's new tail, then the next in order")
                .containsExactly("13:d", "14:e", "15:f");
        assertThat(client.gapsDetected()).as("an overlap is no gap").isZero();
    }
}
