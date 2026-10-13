// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * A takeover's lost range pushed live (M13.33x; ADR-0081 §5): to each of the
 * stream's subscribers as a push naming no segment and carrying no bytes,
 * under its chain sequence -- which a consumer skips rather than waiting on a
 * gap no record will fill -- and recorded delivered, so a session resumes
 * past it.
 */
class SubscriptionHubVoidTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static final class Completing implements SubscriptionHub.Subscriber {
        private final List<SubscriptionHub.Push> completed = new ArrayList<>();

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            return (buffer, offset, length) -> {
                throw new AssertionError("a void writes no bytes");
            };
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            completed.addAll(pushes);
        }
    }

    @Test
    void aVOIDReachesEachSubscriberBytesLessAndASessionResumesPastIt() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Completing session = new Completing();
        List<SubscriptionHub.Push> assembled = new CopyOnWriteArrayList<>();
        try (SubscriptionHub.Session opened = hub.openSession(session, List.of(A));
                var plain = hub.subscribe(A, SubscriptionHub.assembling(assembled::add))) {

            hub.publishVoid(A, 10, 3, 2, 7);

            assertThat(session.completed).singleElement().satisfies(p -> {
                assertThat(p.voided()).isTrue();
                assertThat(p.firstOffset()).isEqualTo(10);
                assertThat(p.recordCount()).isEqualTo(3);
                assertThat(p.sequencerEpoch()).isEqualTo(2);
                assertThat(p.chainSequence()).isEqualTo(7);
                assertThat(p.segment()).isEmpty();
            });
            assertThat(assembled).as("the assembling adapter keeps it a void")
                    .singleElement().satisfies(p -> assertThat(p.voided()).isTrue());
            assertThat(opened.resume(1L, List.of(), List.of()).nextOffset())
                    .as("resumed past the hole").containsEntry(A, 13L);
        }
    }

    @Test
    void aVOIDThatIsNotInlineBytesLessAndChainedIsRefused() {
        assertThatThrownBy(() -> new SubscriptionHub.Push(A, "", 3, 10, FetchMode.INLINE,
                new byte[0], null, 2, SubscriptionHub.CHAIN_SEQUENCE_UNKNOWN))
                .as("no chain sequence").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubscriptionHub.Push(A, "", 3, 10, FetchMode.INLINE,
                new byte[] {1}, null, 2, 7)).as("bytes")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubscriptionHub.Push(A, "", 3, 10, FetchMode.PROXY,
                new byte[0], null, 2, 7)).as("proxy")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
