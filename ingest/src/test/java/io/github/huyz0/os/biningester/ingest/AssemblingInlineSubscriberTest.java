// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * {@link SubscriptionHub#assemblingInline}: only an {@code inline} segment is
 * assembled; any other push arrives complete but with no bytes (M10.3).
 */
class AssemblingInlineSubscriberTest {

    private static final RunKey A = new RunKey(UUID.randomUUID(), 0);
    private static final RunKey B = new RunKey(UUID.randomUUID(), 1);
    private static final long EPOCH = 7;
    private static final long SEQUENCE = 42;

    private static byte[] segmentOf(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    private static List<SubscriptionHub.Push> publish(FetchPolicyConfig policy, byte[] segment)
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("seg", Body.ofBytes(segment));
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
        SubscriptionHub.Subscriber subscriber = SubscriptionHub.assemblingInline(got::add);
        try (var a = hub.subscribe(A, subscriber); var b = hub.subscribe(B, subscriber)) {
            hub.publish(new CommitDelta(SEQUENCE, "seg",
                            List.of(new RunCommit(A, 3, 10), new RunCommit(B, 2, 40))),
                    "seg", segment, new SegmentServing(new FetchPolicy(policy),
                            store.capabilities(), new SegmentProxy(store)), EPOCH);
        }
        return got;
    }

    @Test
    void aProxyPushArrivesForEveryRunWithItsCoordinatesAndNoBytes() throws Exception {
        List<SubscriptionHub.Push> got = publish(new FetchPolicyConfig(1, 0, 1, false),
                segmentOf(300 * 1024));

        assertThat(got).hasSize(2).allSatisfy(push -> {
            assertThat(push.via()).isEqualTo(FetchMode.PROXY);
            assertThat(push.segmentKey()).isEqualTo("seg");
            assertThat(push.segment()).as("no per-subscriber copy of a proxy segment").isEmpty();
        });
        assertThat(got).extracting(SubscriptionHub.Push::key).containsExactlyInAnyOrder(A, B);
        assertThat(got).extracting(SubscriptionHub.Push::firstOffset)
                .containsExactlyInAnyOrder(10L, 40L);
        assertThat(got).extracting(SubscriptionHub.Push::recordCount)
                .containsExactlyInAnyOrder(3, 2);
        assertThat(got).allSatisfy(push -> {
            assertThat(push.sequencerEpoch()).as("the epoch fences a stale push").isEqualTo(EPOCH);
            assertThat(push.chainSequence()).isEqualTo(SEQUENCE);
        });
    }

    @Test
    void aDirectPushKeepsItsGrantAndCarriesNoBytes() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        store.put("seg", Body.ofBytes(segmentOf(64 * 1024)));
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
        try (var a = hub.subscribe(A, SubscriptionHub.assemblingInline(got::add))) {
            // A cold publish (this pod holds a different segment) at fan-out 1,
            // with direct enabled: the policy grants rather than proxies.
            hub.publish(new CommitDelta(SEQUENCE, "seg", List.of(new RunCommit(A, 3, 10))),
                    "seg-some-other-pod-wrote", new byte[] {1},
                    new SegmentServing(new FetchPolicy(new FetchPolicyConfig(1L, 1L, 1, true)),
                            store.capabilities(), new SegmentProxy(store), new GrantIssuer(store)),
                    EPOCH);
        }

        assertThat(got).hasSize(1).allSatisfy(push -> {
            assertThat(push.via()).isEqualTo(FetchMode.DIRECT);
            assertThat(push.grant()).as("the consumer fetches under it").isNotNull();
            assertThat(push.segment()).isEmpty();
            assertThat(push.sequencerEpoch()).isEqualTo(EPOCH);
        });
    }

    @Test
    void aProxyPushIsHandedOneSharedSinkThatCanHoldNothing() throws Exception {
        // Per-session assembly was a per-session buffer; one shared, stateless
        // sink for every non-inline open() cannot be one.
        SubscriptionHub.Subscriber subscriber = SubscriptionHub.assemblingInline(push -> { });
        List<SubscriptionHub.Push> proxy = List.of(new SubscriptionHub.Push(A, "seg", 3, 10,
                FetchMode.PROXY, new byte[0], null, EPOCH, SEQUENCE));
        List<SubscriptionHub.Push> inline = List.of(new SubscriptionHub.Push(A, "seg", 3, 10,
                FetchMode.INLINE, new byte[0], null, EPOCH, SEQUENCE));

        assertThat(subscriber.open(proxy)).isSameAs(subscriber.open(proxy));
        assertThat(subscriber.open(inline))
                .as("an inline open() still gets its own assembling sink")
                .isNotSameAs(subscriber.open(inline));
    }

    @Test
    void anInlinePushStillCarriesTheWholeSegment() throws Exception {
        byte[] segment = segmentOf(4 * 1024);
        List<SubscriptionHub.Push> got = publish(
                new FetchPolicyConfig(Long.MAX_VALUE, Long.MAX_VALUE, 1, false), segment);

        assertThat(got).hasSize(2).allSatisfy(push -> {
            assertThat(push.via()).isEqualTo(FetchMode.INLINE);
            assertThat(push.segment()).isEqualTo(segment);
        });
    }
}
