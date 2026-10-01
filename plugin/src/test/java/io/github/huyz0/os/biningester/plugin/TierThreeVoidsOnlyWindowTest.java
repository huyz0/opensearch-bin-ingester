// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A recovery that only voids, inside a tier-3 window, is not a missing object
 * (M13.25 review round 2, P1).
 *
 * <p>⚠️ IT WAS READ AS ONE: a voids-only recovery has no commits, the window
 * walk took "no commits" for "object missing" and abandoned the whole repair,
 * so no gap on ANY stream with such an entry in its window could be repaired
 * -- held and re-requested at every progress interval for ever.
 */
class TierThreeVoidsOnlyWindowTest {

    private static final String SEGMENT_KEY =
            "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";

    @Test
    void aVOIDSOnlyRecoveryOnAnotherStreamDoesNotAbandonTheRepair() throws Exception {
        RunKey wanted = new RunKey(new UUID(0, 1), 0);
        RunKey other = new RunKey(new UUID(0, 2), 0);
        SegmentWriter writer = new SegmentWriter();
        writer.add(wanted, new SegmentRecord("r0", OpType.INDEX, OptionalLong.of(0),
                "r0".getBytes(StandardCharsets.UTF_8)), 1L);
        byte[] segment = writer.toByteArray(1L);
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        byte[] delta = new CommitDelta(0, SEGMENT_KEY, List.of(new RunCommit(wanted, 1, 0)))
                .encode();
        byte[] voidsOnly = new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(other, 0, 10))).encode();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String key) {
                return OptionalLong.of(checkpoint.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String key) {
                byte[] bytes = key.endsWith("LATEST") ? checkpoint
                        : key.endsWith("0000000000000000.delta") ? delta
                        : key.endsWith("0000000000000001.delta") ? voidsOnly
                        : segment;
                return Optional.of(new ByteArrayInputStream(bytes));
            }
        };
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 1, Map.of(wanted, new TierThreeRecovery.Gap(0, 1)), delivered::add);

        assertThat(complete).as("the needed run is in the store; an unrelated stream's "
                + "voids must not abandon the repair").isTrue();
        assertThat(delivered).hasSize(1);
    }
}
