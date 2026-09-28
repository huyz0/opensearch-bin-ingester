// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The winner fills the cache BEFORE it removes its in-flight entry (M11.14,
 * H9): a caller arriving between the two finds one or the other, never
 * neither -- and "neither" is a second store GET for a segment just read.
 */
class SegmentProxyPutBeforeRemoveTest {

    private static final String KEY = "bins/c/data/put-before-remove.bseg";

    @Test
    void theCacheHoldsTheSegmentBeforeTheInFlightEntryIsRemoved() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = new byte[10_001];
        new Random(14).nextBytes(segment);
        memory.put(KEY, Body.ofBytes(segment));
        SegmentProxy proxy = new SegmentProxy(memory, 1024, SegmentCache.forSegmentsOf(1 << 20));
        AtomicReference<byte[]> cachedAtRemoval = new AtomicReference<>();
        proxy.betweenCompleteAndRemove = () -> cachedAtRemoval.set(proxy.cache().get(KEY));
        ByteArrayOutputStream got = new ByteArrayOutputStream();

        proxy.streamTo(KEY, List.of(got::write));

        assertThat(got.toByteArray()).as("the premise: the read completed").isEqualTo(segment);
        assertThat(cachedAtRemoval.get())
                .as("⚠️ FILLED BEFORE THE ENTRY GOES: a caller between the two finds the cache")
                .isEqualTo(segment);
    }
}
