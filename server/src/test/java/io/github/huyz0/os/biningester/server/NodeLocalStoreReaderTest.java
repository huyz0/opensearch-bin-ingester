// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.MembershipFilter;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.sequencer.NodeLocalStoreReaderKeyPolicy;
import java.io.IOException;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class NodeLocalStoreReaderTest {
    private static final String BUCKET = "events-bucket";
    private static final String PREFIX = "cluster-a/ingest";
    private static final String DELTA = PREFIX + "/ctl/log/0/0000000000000011/000000000000002a.delta";
    private static final String LATEST = PREFIX + "/ctl/log/0/0000000000000011/ckpt/LATEST";
    private static final byte[] SECRET = "12345678901234567890123456789012".getBytes();

    @Test
    void rejectsAuthenticationAndNamespaceBeforeAnyStoreCall() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        String key = DELTA;
        memory.put(key, Body.ofBytes(new byte[] {1, 2, 3}));
        CountingBinStore store = new CountingBinStore(memory);
        NodeLocalStoreReader reader = reader(store, SECRET, 64);

        assertThatThrownBy(() -> reader.get(null, BUCKET, PREFIX, key))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> reader.get("Bearer invalid", BUCKET, PREFIX, key))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> reader.get(auth(SECRET), "other-bucket", PREFIX, key))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.get(auth(SECRET), BUCKET, "other-prefix", key))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.counts().total()).isZero();
    }

    @Test
    void streamsAtMostTheConfiguredObjectSizeWithoutDisclosingBytes() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        String key = new SegmentKey(PREFIX, 1_700_000_000_000L, "pod7", 42, 96,
                new MembershipFilter.None().encode()).key();
        byte[] payload = "secret-payload".getBytes();
        memory.put(key, Body.ofBytes(payload));
        CountingBinStore store = new CountingBinStore(memory);
        NodeLocalStoreReader reader = reader(store, SECRET, 4);

        try (var body = reader.get(auth(SECRET), BUCKET, PREFIX, key)) {
            assertThatThrownBy(body::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasMessageNotContaining("secret-payload");
        }
        assertThat(store.counts().gets()).isEqualTo(1);
        assertThat(store.counts().lists()).isZero();

        String exactKey = new SegmentKey(PREFIX, 1_700_000_000_001L, "pod7", 43, 96).key();
        memory.put(exactKey, Body.ofBytes(new byte[] {1, 2, 3, 4}));
        try (var scalar = reader.get(auth(SECRET), BUCKET, PREFIX, exactKey)) {
            assertThat(scalar.read()).isEqualTo(1);
        }
        try (var exact = reader.get(auth(SECRET), BUCKET, PREFIX, exactKey)) {
            assertThat(exact.read(new byte[0], 0, 0)).isZero();
            assertThat(exact.readNBytes(4)).containsExactly(1, 2, 3, 4);
            assertThat(exact.read()).isEqualTo(-1);
        }
        String partialKey = new SegmentKey(PREFIX, 1_700_000_000_002L, "pod7", 44, 96).key();
        memory.put(partialKey, Body.ofBytes(new byte[] {1, 2, 3, 4, 5, 6}));
        try (var partial = reader.get(auth(SECRET), BUCKET, PREFIX, partialKey)) {
            assertThat(partial.readNBytes(4)).containsExactly(1, 2, 3, 4);
            assertThatThrownBy(partial::read).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> partial.read(new byte[1], 0, 1))
                    .isInstanceOf(IOException.class);
        }
        String zeroOverflowKey = new SegmentKey(PREFIX, 1_700_000_000_003L, "pod7", 45, 96).key();
        memory.put(zeroOverflowKey, Body.ofBytes(new byte[] {1, 2, 3, 4, 0}));
        try (var zeroOverflow = reader.get(auth(SECRET), BUCKET, PREFIX, zeroOverflowKey)) {
            assertThat(zeroOverflow.readNBytes(4)).containsExactly(1, 2, 3, 4);
            assertThatThrownBy(zeroOverflow::read).isInstanceOf(IOException.class);
        }
        try (var zeroBulkOverflow = reader.get(auth(SECRET), BUCKET, PREFIX, zeroOverflowKey)) {
            assertThat(zeroBulkOverflow.readNBytes(4)).containsExactly(1, 2, 3, 4);
            assertThatThrownBy(() -> zeroBulkOverflow.read(new byte[1], 0, 1))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void permitsStatOnlyOnLatestCheckpointAndRefusesOtherKeysBeforeIo() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        memory.put(LATEST, Body.ofBytes(new byte[] {1}));
        memory.put(DELTA, Body.ofBytes(new byte[] {2}));
        CountingBinStore store = new CountingBinStore(memory);
        NodeLocalStoreReader reader = reader(store, SECRET, 64);

        assertThatThrownBy(() -> reader.stat(null, BUCKET, PREFIX, LATEST))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> reader.stat("Bearer invalid", BUCKET, PREFIX, LATEST))
                .isInstanceOf(SecurityException.class);
        assertThat(store.counts().stats()).isZero();
        assertThat(reader.stat(auth(SECRET), BUCKET, PREFIX, LATEST))
                .isPresent();
        assertThatThrownBy(() -> reader.stat(auth(SECRET), BUCKET, PREFIX, DELTA))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.get(auth(SECRET), BUCKET, PREFIX, "other/key"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.counts().stats()).isEqualTo(1);
        assertThat(store.counts().gets()).isZero();
        assertThat(store.counts().lists()).isZero();
    }

    @Test
    void refusesInvalidSecretAndObjectLimitBounds() {
        MemoryBinStore memory = new MemoryBinStore();
        var policy = new NodeLocalStoreReaderKeyPolicy(BUCKET, PREFIX);
        assertThatThrownBy(() -> new NodeLocalStoreReader(memory, policy, new byte[31], 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReader(memory, policy, SECRET, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReader(memory, policy, SECRET,
                NodeLocalStoreReader.DEFAULT_MAX_OBJECT_BYTES + 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new NodeLocalStoreReader(memory, policy, SECRET, 1)).isNotNull();
        assertThat(new NodeLocalStoreReader(memory, policy, SECRET,
                NodeLocalStoreReader.DEFAULT_MAX_OBJECT_BYTES)).isNotNull();
    }

    private static NodeLocalStoreReader reader(CountingBinStore store, byte[] secret,
            long maxObjectBytes) {
        return new NodeLocalStoreReader(store,
                new NodeLocalStoreReaderKeyPolicy(BUCKET, PREFIX), secret, maxObjectBytes);
    }

    private static String auth(byte[] secret) {
        return "Bearer " + Base64.getUrlEncoder().withoutPadding().encodeToString(secret);
    }
}
