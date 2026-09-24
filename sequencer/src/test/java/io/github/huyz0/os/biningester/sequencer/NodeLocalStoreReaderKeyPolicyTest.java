// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.MembershipFilter;
import io.github.huyz0.os.biningester.format.SegmentKey;
import org.junit.jupiter.api.Test;

class NodeLocalStoreReaderKeyPolicyTest {
    private static final String BUCKET = "events-bucket";
    private static final String PREFIX = "cluster-a/ingest";
    private static final NodeLocalStoreReaderKeyPolicy POLICY =
            new NodeLocalStoreReaderKeyPolicy(BUCKET, PREFIX);
    private static final LogKeys LOG = new LogKeys(PREFIX, 17);

    @Test
    void acceptsOnlyCanonicalRecoveryKeysAndStatOnlyForLatestCheckpoint() {
        String pointer = LOG.latestCheckpointKey();
        String delta = LOG.keyFor(42);
        String segment = new SegmentKey(PREFIX, 1_700_000_000_000L, "pod7", 42, 96,
                new MembershipFilter.None().encode()).key();

        assertThat(POLICY.authorize(BUCKET, PREFIX, pointer,
                NodeLocalStoreReaderKeyPolicy.Operation.GET))
                .isEqualTo(NodeLocalStoreReaderKeyPolicy.KeyKind.CHECKPOINT_POINTER);
        assertThat(POLICY.authorize(BUCKET, PREFIX, pointer,
                NodeLocalStoreReaderKeyPolicy.Operation.STAT))
                .isEqualTo(NodeLocalStoreReaderKeyPolicy.KeyKind.CHECKPOINT_POINTER);
        assertThat(POLICY.authorize(BUCKET, PREFIX, delta,
                NodeLocalStoreReaderKeyPolicy.Operation.GET))
                .isEqualTo(NodeLocalStoreReaderKeyPolicy.KeyKind.COMMIT_DELTA);
        assertThat(POLICY.authorize(BUCKET, PREFIX, segment,
                NodeLocalStoreReaderKeyPolicy.Operation.GET))
                .isEqualTo(NodeLocalStoreReaderKeyPolicy.KeyKind.SEGMENT);
        assertThatThrownBy(() -> POLICY.authorize(BUCKET, PREFIX, delta,
                NodeLocalStoreReaderKeyPolicy.Operation.STAT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> POLICY.authorize(BUCKET, PREFIX, segment,
                NodeLocalStoreReaderKeyPolicy.Operation.STAT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesNoncanonicalKeysAndCallerSelectedNamespaces() {
        String deltaPrefix = LOG.logPrefix();
        String pointer = LOG.latestCheckpointKey();
        String canonicalDelta = LOG.keyFor(9);
        String canonicalSegment = new SegmentKey(PREFIX, 1_700_000_000_000L, "pod7", 42, 96,
                new MembershipFilter.None().encode()).key();

        assertRefused(BUCKET, PREFIX, deltaPrefix + "9.delta", NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, PREFIX, deltaPrefix + "000000000000000A.delta",
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, PREFIX, deltaPrefix + "ckpt/0000000000000009.ckpt",
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, PREFIX, "s3://" + BUCKET + "/" + canonicalDelta,
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, PREFIX, PREFIX + "/../" + canonicalDelta,
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused("other-bucket", PREFIX, pointer, NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, "other-prefix", canonicalSegment,
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
        assertRefused(BUCKET, PREFIX, canonicalSegment + "/child",
                NodeLocalStoreReaderKeyPolicy.Operation.GET);
    }

    private static void assertRefused(String bucket, String prefix, String key,
            NodeLocalStoreReaderKeyPolicy.Operation operation) {
        assertThatThrownBy(() -> POLICY.authorize(bucket, prefix, key, operation))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
