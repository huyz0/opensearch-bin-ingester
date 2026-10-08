// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A void reaches a subscriber as a counted skip, not an upstream gap (M13.25e,
 * ADR-0082 §5's last bullet).
 *
 * <p>⚠️ **A TAKEOVER's VOID IS A COMMITTED HOLE**: offsets the chain says no
 * record will ever hold. Without an event saying so, a consumer read the next
 * push past it as records lost upstream, and the plugin held the stream and
 * retried a repair that could never complete -- a permanent stall. A void
 * event is v5: a v4 body and one trailing byte, so every event that is not a
 * void keeps its v1-v4 bytes, and an old reader refuses rather than misreads it.
 */
class SubscriptionEventVoidTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666"), 7);

    private static SubscriptionEvent aVoid() {
        return SubscriptionEvent.voidRange("sess-abc", 42L, 3L, KEY, 1_000L, 128, 9_876L);
    }

    private static byte[] golden(String name) throws IOException {
        try (var in = SubscriptionEventVoidTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aVOIDMatchesItsGoldenBytesAndRoundTrips() throws Exception {
        byte[] stored = golden("subscription-event-void-v5.bin");

        assertThat(aVoid().encode()).isEqualTo(stored);
        SubscriptionEvent decoded = SubscriptionEvent.decode(stored);
        assertThat(decoded).isEqualTo(aVoid());
        assertThat(decoded.voided()).isTrue();
        assertThat(decoded.firstOffset()).isEqualTo(1_000L);
        assertThat(decoded.recordCount()).as("the void's width").isEqualTo(128);
        assertThat(java.nio.ByteBuffer.wrap(stored, 4, 4).getInt())
                .as("version 5 on the wire").isEqualTo(SubscriptionEvent.VERSION_5);
    }

    @Test
    void aVOIDIsNotEqualToRecordsAtTheSameOffsets() {
        SubscriptionEvent records = new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "",
                1_000L, 128, FetchMode.PROXY, new byte[0], null,
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT, 9_876L);

        assertThat(aVoid()).isNotEqualTo(records);
        assertThat(records.voided()).isFalse();
    }

    @Test
    void aVOIDNeedsAChainSequenceAndASessionEpoch() {
        assertThatThrownBy(() -> SubscriptionEvent.voidRange("sess-abc", 42L, 3L, KEY,
                1_000L, 128, SubscriptionEvent.CHAIN_SEQUENCE_ABSENT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubscriptionEvent.voidRange("sess-abc", 42L,
                SubscriptionEvent.SESSION_EPOCH_ABSENT, KEY, 1_000L, 128, 9_876L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aVOIDCarryingASegmentBytesOrAGrantIsRefused() {
        // ⚠️ M13.25e review T3.
        assertThatThrownBy(() -> new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg",
                1_000L, 128, FetchMode.INLINE, new byte[0], null, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT, 9_876L, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "",
                1_000L, 128, FetchMode.INLINE, new byte[] {1}, null, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT, 9_876L, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "",
                1_000L, 128, FetchMode.PROXY, new byte[0], null, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT, 9_876L, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aV5BodyWhoseVoidByteIsNotOneIsRefused() throws Exception {
        byte[] stored = golden("subscription-event-void-v5.bin");
        for (byte flag : new byte[] {0, 2}) {
            byte[] bent = stored.clone();
            bent[bent.length - 1] = flag;

            assertThatThrownBy(() -> SubscriptionEvent.decode(bent))
                    .as("void byte %d", flag).isInstanceOf(IOException.class);
        }
    }
}
