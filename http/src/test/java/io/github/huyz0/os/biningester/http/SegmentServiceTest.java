// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.SegmentCache;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentReads;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The proxy segment route over a real socket (M10.1, FR-6, NFR-5, NFR-6,
 * ADR-0073).
 */
class SegmentServiceTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String HERE = "az-a";
    private static final int CHUNK = 64 * 1024;
    private static final int SEGMENT_BYTES = 5 * CHUNK + 123;

    private static final String KEY =
            new SegmentKey(PREFIX, 1_700_000_000_000L, "pod7", 42, 96).key();

    private WebServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static byte[] segment() {
        byte[] bytes = new byte[SEGMENT_BYTES];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 31 + (i >> 8));
        }
        return bytes;
    }

    private String start(CountingBinStore store, CrossAzBytes counter) {
        SegmentReads reads = new SegmentReads(new SegmentProxy(store, CHUNK,
                new SegmentCache(4L * SEGMENT_BYTES)));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new SegmentService(reads::serve, PREFIX, counter)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private HttpResponse<byte[]> get(String endpoint, String key, String az) throws Exception {
        StringBuilder uri = new StringBuilder(endpoint).append(SegmentService.PATH);
        String separator = "?";
        if (key != null) {
            uri.append(separator).append(SegmentService.KEY_PARAM).append('=')
                    .append(URLEncoder.encode(key, StandardCharsets.UTF_8));
            separator = "&";
        }
        if (az != null) {
            uri.append(separator).append(SegmentService.AZ_PARAM).append('=').append(az);
        }
        return http.send(HttpRequest.newBuilder(URI.create(uri.toString())).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static CountingBinStore holding(String key, byte[] bytes) throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(key, Body.ofBytes(bytes));
        return store;
    }

    @Test
    void aSegmentLargerThanAChunkIsServedWhole() throws Exception {
        String endpoint = start(holding(KEY, segment()), new CrossAzBytes(HERE));

        HttpResponse<byte[]> response = get(endpoint, KEY, HERE);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(segment());
    }

    @Test
    void onlyThisDeploymentsSegmentKeysAreAddressable() throws Exception {
        CountingBinStore store = holding(KEY, segment());
        String foreign = new SegmentKey("bins/cluster-b", 1_700_000_000_000L, "pod7", 42, 96).key();
        store.put(foreign, Body.ofBytes(segment()));
        store.put(PREFIX + "/ctl/lease", Body.ofBytes("secret".getBytes(StandardCharsets.UTF_8)));
        CrossAzBytes counter = new CrossAzBytes(HERE);
        String endpoint = start(store, counter);
        long getsBefore = store.counts().gets();

        List<String> refused = List.of(
                foreign,
                PREFIX + "/ctl/lease",
                PREFIX + "/log/0000000000000000001",
                KEY + "/../../ctl/lease",
                "not-a-key",
                "",
                PREFIX + "/" + "x".repeat(SegmentKey.MAX_KEY_BYTES) + ".bseg");
        for (String key : refused) {
            HttpResponse<byte[]> response = get(endpoint, key, HERE);
            assertThat(response.statusCode()).as("key %s", key).isEqualTo(400);
            String body = new String(response.body(), StandardCharsets.UTF_8);
            if (!key.isEmpty()) {
                assertThat(body).as("the refusal does not echo the key").doesNotContain(key);
            }
        }
        assertThat(get(endpoint, null, HERE).statusCode()).as("no key at all").isEqualTo(400);
        assertThat(store.counts().gets() - getsBefore)
                .as("a refused key never reaches the store").isZero();
        assertThat(counter.crossAzBytes() + counter.sameAzBytes())
                .as("a refusal is not a proxy read").isZero();
    }

    @Test
    void aStoreFailureBeforeTheFirstByteIs503() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        String endpoint = start(new CountingBinStore(new MemoryBinStore()), counter);

        HttpResponse<byte[]> response = get(endpoint, KEY, "az-b");

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).as("never an empty segment under 200").isNotEmpty();
        assertThat(counter.crossAzBytes(Transport.PROXY_READ))
                .as("an error body is not a proxy read").isZero();
    }

    @Test
    void aFailureAfterTheFirstByteTruncatesTheBodyRatherThanEndingIt() throws Exception {
        // Five chunks, past Helidon's own buffering, then the store fails: the
        // status is already 200, so the only honest signal left is a chunked
        // body that never terminates. Ending it cleanly would hand the consumer
        // a short segment under 200.
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new SegmentService((key, sink) -> {
                            byte[] chunk = new byte[CHUNK];
                            for (int i = 0; i < 5; i++) {
                                sink.write(chunk, 0, chunk.length);
                            }
                            throw new java.io.IOException("the store connection reset");
                        }, PREFIX, new CrossAzBytes(HERE))))
                .build().start();

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> get("http://localhost:" + server.port(), KEY, HERE))
                .as("the client sees a broken body, not a complete short one")
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void everyBodyByteIsCountedOnceAsProxyReadAgainstTheConsumersZone() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        String endpoint = start(holding(KEY, segment()), counter);

        assertThat(get(endpoint, KEY, "az-b").body()).hasSize(SEGMENT_BYTES);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ))
                .as("a consumer in another zone was sent the whole segment across it")
                .isEqualTo(SEGMENT_BYTES);
        assertThat(counter.sameAzBytes(Transport.PROXY_READ)).isZero();

        assertThat(get(endpoint, KEY, HERE).body()).hasSize(SEGMENT_BYTES);
        assertThat(counter.sameAzBytes(Transport.PROXY_READ))
                .as("the same read from this zone is same-AZ, not cross-AZ")
                .isEqualTo(SEGMENT_BYTES);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isEqualTo(SEGMENT_BYTES);

        assertThat(get(endpoint, KEY, null).body()).hasSize(SEGMENT_BYTES);
        assertThat(counter.unknownPeerBytes())
                .as("a consumer that names no zone is counted as unknown, the safe side")
                .isEqualTo(SEGMENT_BYTES);
    }
}
