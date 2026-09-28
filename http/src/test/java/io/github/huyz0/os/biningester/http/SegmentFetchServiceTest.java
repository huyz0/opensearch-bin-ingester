// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.SegmentCache;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The proxy segment route (M10.1, ADR-0073, M10 criteria 1 and 2).
 *
 * <p>⚠️ OVER A REAL SOCKET AND A COUNTING STORE, because the two properties
 * that matter are both COUNTS: how many store requests a fetch costs, and how
 * many bytes crossed which zone boundary. A fake streamer would assert the
 * route's arithmetic back at itself.
 */
@org.junit.jupiter.api.Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS,
        threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentFetchServiceTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String HERE = "az-a";
    private static final long T = 1_790_000_000_000L;

    private WebServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static String segmentKey(String prefix, long sequence) {
        return new SegmentKey(prefix, T, "pod7", sequence, 96).key();
    }

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    private String start(BinStore store, CrossAzBytes counter) {
        SegmentProxy proxy = new SegmentProxy(store, 4096,
                SegmentCache.forSegmentsOf(1 << 20));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(
                        SegmentFetchService.over(PREFIX, proxy, store, counter)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private HttpResponse<byte[]> fetch(String endpoint, String key, String az) throws Exception {
        StringBuilder uri = new StringBuilder(endpoint).append(SegmentFetchRoute.PATH);
        String sep = "?";
        if (key != null) {
            uri.append(sep).append(SegmentFetchRoute.KEY_PARAM).append('=')
                    .append(URLEncoder.encode(key, StandardCharsets.UTF_8));
            sep = "&";
        }
        if (az != null) {
            uri.append(sep).append(SegmentFetchRoute.AZ_PARAM).append('=').append(az);
        }
        return http.send(HttpRequest.newBuilder(URI.create(uri.toString())).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void theWholeStoredSegmentIsServedAndARepeatIsACacheHit() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String key = segmentKey(PREFIX, 1);
        byte[] segment = bytes(10_000);
        store.put(key, Body.ofBytes(segment));
        String endpoint = start(store, new CrossAzBytes(HERE));
        long getsBefore = store.counts().gets();

        HttpResponse<byte[]> first = fetch(endpoint, key, HERE);
        HttpResponse<byte[]> second = fetch(endpoint, key, HERE);

        assertThat(first.statusCode()).isEqualTo(200);
        assertThat(first.body()).as("the whole object, byte for byte").isEqualTo(segment);
        assertThat(second.statusCode()).isEqualTo(200);
        assertThat(second.body()).isEqualTo(segment);
        assertThat(store.counts().gets() - getsBefore)
                .as("⚠️ ONE GET FOR TWO FETCHES: the route reads through the node-wide "
                        + "cache, so a node serving N consumers costs one store read")
                .isEqualTo(1);
    }

    @Test
    void aKeyOutsideThisDomainsDataGrammarIsRefusedBeforeAnyStoreRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String lease = PREFIX + "/ctl/lease/0.json";
        String otherDomain = segmentKey("bins/cluster-b", 1);
        store.put(lease, Body.ofBytes(bytes(10)));
        store.put(otherDomain, Body.ofBytes(bytes(10)));
        String endpoint = start(store, new CrossAzBytes(HERE));
        StoreCounts before = store.counts();

        for (String key : new String[] {lease, otherDomain, PREFIX + "/data/../ctl/lease/0.json",
                segmentKey(PREFIX, 2) + "x", ""}) {
            assertThat(fetch(endpoint, key, HERE).statusCode()).as(key).isEqualTo(400);
        }
        assertThat(fetch(endpoint, null, HERE).statusCode()).as("no key at all").isEqualTo(400);
        assertThat(store.counts())
                .as("⚠️ REFUSED BEFORE I/O: an unauthenticated route that read any key "
                        + "it was named would serve leases and other domains' data")
                .isEqualTo(before);
    }

    @Test
    void anAbsentSegmentIsNotFound() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String endpoint = start(store, new CrossAzBytes(HERE));

        HttpResponse<byte[]> answer = fetch(endpoint, segmentKey(PREFIX, 9), HERE);

        assertThat(answer.statusCode()).isEqualTo(404);
    }

    @Test
    void aStoreThatFailsWhileTheObjectExistsIsNotReportedAsAbsent() throws Exception {
        // ⚠️ A 404 is PERMANENT to a consumer; an outage reported as one would
        // make it stop asking for a segment that is there.
        MemoryBinStore memory = new MemoryBinStore();
        String key = segmentKey(PREFIX, 3);
        memory.put(key, Body.ofBytes(bytes(100)));
        java.util.concurrent.atomic.AtomicBoolean down =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        BinStore failingGets = (BinStore) Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (self, method, args) -> {
                    if (method.getName().equals("get") && down.get()) {
                        throw new IOException("the store is unreachable");
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        String endpoint = start(failingGets, new CrossAzBytes(HERE));

        HttpResponse<byte[]> answer = fetch(endpoint, key, HERE);

        assertThat(answer.statusCode()).isEqualTo(502);
        down.set(false);
        assertThat(fetch(endpoint, key, HERE).statusCode())
                .as("⚠️ AN OUTAGE IS NEVER REMEMBERED AS ABSENCE: once the store answers, "
                        + "the segment is served")
                .isEqualTo(200);
    }

    @Test
    void theBytesServedAreCountedAgainstTheCallersZone() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String key = segmentKey(PREFIX, 4);
        byte[] segment = bytes(7_777);
        store.put(key, Body.ofBytes(segment));
        CrossAzBytes counter = new CrossAzBytes(HERE);
        String endpoint = start(store, counter);

        fetch(endpoint, key, HERE);
        assertThat(counter.sameAzBytes(Transport.PROXY_READ)).isEqualTo(segment.length);
        assertThat(counter.crossAzBytes())
                .as("a same-zone fetch is not NFR-5's numerator").isZero();

        fetch(endpoint, key, "az-b");
        assertThat(counter.crossAzBytes(Transport.PROXY_READ))
                .as("⚠️ EXACTLY the payload, counted where it was sent")
                .isEqualTo(segment.length);
        assertThat(counter.unknownPeerBytes()).isZero();

        fetch(endpoint, key, null);
        assertThat(counter.unknownPeerBytes())
                .as("a caller that names no zone is assumed cross-AZ, never same-AZ")
                .isEqualTo(segment.length);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isEqualTo(2L * segment.length);
    }

    @Test
    void aStoreThatCannotBeAskedAtAllIsABadGatewayNeverAbsent() throws Exception {
        // ⚠️ A REAL OUTAGE FAILS THE GET AND THE STAT ALIKE, and only a 502
        // keeps the consumer asking for a segment that is still there.
        BinStore down = (BinStore) Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (self, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new IOException("the store is unreachable");
                });
        String endpoint = start(down, new CrossAzBytes(HERE));

        assertThat(fetch(endpoint, segmentKey(PREFIX, 5), HERE).statusCode()).isEqualTo(502);
        assertThat(fetch(endpoint, segmentKey(PREFIX, 5), HERE).statusCode())
                .as("asked again while still down: still an outage, never remembered as absent")
                .isEqualTo(502);
    }

    @Test
    void anAbsentKeyIsRememberedSoARetryCostsNoStoreRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String endpoint = start(store, new CrossAzBytes(HERE));
        String key = segmentKey(PREFIX, 6);

        assertThat(fetch(endpoint, key, HERE).statusCode()).isEqualTo(404);
        StoreCounts afterFirst = store.counts();
        assertThat(fetch(endpoint, key, HERE).statusCode()).isEqualTo(404);

        assertThat(store.counts()).as("a retry of a segment GC deleted buys nothing")
                .isEqualTo(afterFirst);
        assertThat(afterFirst.gets() + afterFirst.stats()).isEqualTo(2);
    }

    @Test
    void theMemoryOfAbsentKeysIsBounded() throws Exception {
        // ⚠️ AN UNAUTHENTICATED CALLER INVENTING KEYS must not grow the heap
        // without limit: past the bound the oldest absence is forgotten, and
        // asking for it again costs the store requests once more.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String endpoint = start(store, new CrossAzBytes(HERE));
        String first = segmentKey(PREFIX, 1_000_000);
        fetch(endpoint, first, HERE);
        for (int i = 1; i <= SegmentFetchService.ABSENT_REMEMBERED; i++) {
            assertThat(fetch(endpoint, segmentKey(PREFIX, 1_000_000 + i), HERE).statusCode())
                    .isEqualTo(404);
        }
        StoreCounts before = store.counts();

        assertThat(fetch(endpoint, first, HERE).statusCode()).isEqualTo(404);

        assertThat(store.counts().gets()).as("the first absence was forgotten")
                .isEqualTo(before.gets() + 1);
    }

    @Test
    void aZeroLengthObjectIsServedAsStored() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        String key = segmentKey(PREFIX, 7);
        store.put(key, Body.ofBytes(new byte[0]));
        String endpoint = start(store, new CrossAzBytes(HERE));

        HttpResponse<byte[]> answer = fetch(endpoint, key, HERE);

        assertThat(answer.statusCode()).isEqualTo(200);
        assertThat(answer.body()).isEmpty();
    }

    private String startStreaming(SegmentFetchService.Streamer streamer, CrossAzBytes counter) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new SegmentFetchService(PREFIX,
                        streamer, key -> true, counter)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private URI uri(String endpoint, String key) {
        return URI.create(endpoint + SegmentFetchRoute.PATH + "?" + SegmentFetchRoute.KEY_PARAM
                + "=" + URLEncoder.encode(key, StandardCharsets.UTF_8) + "&"
                + SegmentFetchRoute.AZ_PARAM + "=" + HERE);
    }

    @Test
    void theFirstChunkReachesTheConsumerBeforeTheStoreHasFinished() throws Exception {
        // ⚠️ STREAMED, NOT BUFFERED THEN FORWARDED (ADR-0073, research 10 §1):
        // the consumer holds the first chunk while the store is still reading.
        java.util.concurrent.CountDownLatch rest = new java.util.concurrent.CountDownLatch(1);
        byte[] chunk = bytes(100);
        String endpoint = startStreaming((key, sink) -> {
            sink.write(chunk, 0, chunk.length);
            try {
                rest.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sink.write(chunk, 0, chunk.length);
        }, new CrossAzBytes(HERE));
        // ⚠️ THE HEADER WAIT IS BOUNDED TOO (M11.14, H9): a server that buffers
        // before answering holds the headers as well as the first chunk, and
        // an unbounded `send` would wait on the latch released below for ever.
        var answer = http.sendAsync(
                HttpRequest.newBuilder(uri(endpoint, segmentKey(PREFIX, 8))).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        java.io.InputStream in = null;
        byte[] got;
        try {
            java.io.InputStream body = answer.get(10, java.util.concurrent.TimeUnit.SECONDS)
                    .body();
            in = body;
            var first = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return body.readNBytes(chunk.length);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            got = first.get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException buffered) {
            got = null;
        } finally {
            // ⚠️ RELEASED BEFORE THE STREAM IS CLOSED: closing waits for the
            // handler, and the handler waits for this latch.
            rest.countDown();
        }
        assertThat(got).as("the first chunk arrived while the second was still being read")
                .isEqualTo(chunk);
        assertThat(in.readAllBytes()).isEqualTo(chunk);
        in.close();
    }

    @Test
    void aStoreFailureMidSegmentAbortsTheStreamAndCountsWhatWasSent() throws Exception {
        // ⚠️ AT SIZES EITHER SIDE OF THE SERVER'S OWN BUFFER: below it a failure
        // was once answered as a complete 500 with a body, which a consumer
        // cannot tell from an answer.
        for (int written : new int[] {100, 70_000, 4 << 20}) {
            CrossAzBytes counter = new CrossAzBytes(HERE);
            byte[] part = bytes(written);
            String endpoint = startStreaming((key, sink) -> {
                sink.write(part, 0, part.length);
                throw new IOException("the store failed mid-segment");
            }, counter);

            IOException aborted = null;
            int status = -1;
            try {
                status = http.send(
                        HttpRequest.newBuilder(uri(endpoint, segmentKey(PREFIX, 9))).GET().build(),
                        HttpResponse.BodyHandlers.ofByteArray()).statusCode();
            } catch (IOException e) {
                aborted = e;
            }

            assertThat(aborted).as("after %d bytes a failure is an ABORTED stream, never an "
                    + "answer -- a clean %d would read as one", written, status).isNotNull();
            assertThat(counter.sameAzBytes(Transport.PROXY_READ))
                    .as("the bytes written before the failure were sent").isEqualTo(written);
            server.stop();
            server = null;
        }
    }
}
