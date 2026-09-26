// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The node that wrote a segment serves it through the proxy route without a
 * store GET (M10.15, NFR-4, ADR-0004: "the writing AZ needs no GET").
 */
class WriterCacheAdmissionTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);
    private static final int CHUNK = 64 * 1024;

    private static byte[] segmentOf(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    private static CommitDelta oneRun(String segmentKey) {
        return new CommitDelta(1, segmentKey, List.of(new RunCommit(KEY, 3, 10)));
    }

    private static final class Collecting implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            out.write(buffer, offset, length);
        }
    }

    private record Node(CountingBinStore store, SubscriptionHub hub, SegmentServing serving,
            SegmentReads reads) {
    }

    private static Node node(String segmentKey, byte[] segment, long cacheBytes)
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        // The object exists, so a GET WOULD succeed: the count below is what
        // tells "served from the held bytes" from "read it back".
        store.put(segmentKey, Body.ofBytes(segment));
        SegmentProxy proxy = new SegmentProxy(store, CHUNK, new SegmentCache(cacheBytes));
        SegmentServing serving = new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), proxy);
        return new Node(store, new SubscriptionHub(), serving, new SegmentReads(proxy));
    }

    @Test
    void theWriterServesItsOwnSegmentWithoutAGet() throws Exception {
        byte[] segment = segmentOf(1024 * 1024);
        Node writer = node("seg-held", segment, 4L * segment.length);
        try (var ignored = writer.hub().subscribe(KEY, SubscriptionHub.assembling(push -> { }))) {
            writer.hub().publish(oneRun("seg-held"), "seg-held", segment, writer.serving());
        }
        long before = writer.store().counts().gets();

        Collecting consumer = new Collecting();
        assertThat(writer.reads().serve("seg-held", consumer)).isTrue();

        assertThat(consumer.out.toByteArray()).isEqualTo(segment);
        assertThat(writer.store().counts().gets() - before)
                .as("the writer holds the bytes it wrote; serving them is not a GET")
                .isZero();
    }

    @Test
    void aHeldSegmentIsAdmittedEvenWhenThisNodeHasNoSubscriberForIt() throws Exception {
        // The consumer that will fetch it polls another node's subscription,
        // or has not subscribed yet; the writer must still hold it for /seg.
        byte[] segment = segmentOf(300 * 1024);
        Node writer = node("seg-unwatched", segment, 4L * segment.length);

        writer.hub().publish(oneRun("seg-unwatched"), "seg-unwatched", segment, writer.serving());

        Collecting consumer = new Collecting();
        assertThat(writer.reads().serve("seg-unwatched", consumer)).isTrue();
        assertThat(consumer.out.toByteArray()).isEqualTo(segment);
        assertThat(writer.store().counts().gets()).isZero();
    }
}
