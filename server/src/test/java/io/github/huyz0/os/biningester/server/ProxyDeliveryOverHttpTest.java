// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.HttpProxySource;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.ProxySource;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * M10 criterion 1: an assembled ingester with DEFAULT fetch settings serves a
 * segment above the 256 KiB inline cap as {@code proxy}, and a consumer over
 * HTTP decodes every record (M10.2, FR-6, ADR-0073). Before M10 this consumer
 * threw decoding the empty array a {@code proxy} event carries.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ProxyDeliveryOverHttpTest {

    private static final String INDEX = "logs";
    private static final String HERE = "az-a";
    private static final int RECORDS = 400;
    private static final String PAD = "p".repeat(2048);

    @TempDir
    Path dir;

    private Path configFile() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.uid=uid-pod1",
                "pod.az=" + HERE,
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=memory",
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=" + INDEX,
                "ingest.interval-floor=PT0.05S",
                "").getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String indexUuid(UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    private static String bulkBody() {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < RECORDS; i++) {
            body.append("{\"index\":{\"_id\":\"doc-").append(i).append("\",\"_version\":1}}\n")
                    .append("{\"n\":").append(i).append(",\"pad\":\"").append(PAD)
                    .append("\"}\n");
        }
        return body.toString();
    }

    @Test
    void aSegmentAboveTheInlineCapIsDecodedThroughTheProxyRoute() throws Exception {
        UUID stream = UUID.randomUUID();
        try (IngesterNode node = Main.run(configFile().toString())) {
            node.assembly().catalog().register(
                    new IndexRegistration(indexUuid(stream), INDEX, List.of(), 1, 1, 1, 1));
            String endpoint = "http://localhost:" + node.port();
            AtomicInteger fetches = new AtomicInteger();
            HttpProxySource route = new HttpProxySource(endpoint, Duration.ofSeconds(30), HERE);
            ProxySource counted = segmentKey -> {
                fetches.incrementAndGet();
                return route.fetch(segmentKey);
            };
            HttpSubscriptionTransport transport = new HttpSubscriptionTransport(endpoint,
                    () -> { }, Duration.ofMillis(50), Duration.ofSeconds(1),
                    Duration.ofSeconds(30), Duration.ofSeconds(2), 32 * 1024 * 1024, HERE);
            int read = 0;
            try (ConsumerClient consumer = new ConsumerClient(transport, new RunKey(stream, 0),
                    256, null, counted)) {
                int status = WebClient.builder().baseUri(endpoint)
                        .readTimeout(Duration.ofSeconds(30)).build()
                        .post("/" + INDEX + "/_bulk").queryParam("partition", "0")
                        .submit(bulkBody()).status().code();
                assertThat(status).isEqualTo(202);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (read < RECORDS && System.nanoTime() < deadline) {
                    Optional<ConsumerRecord> next = consumer.readNext(Duration.ofSeconds(1));
                    if (next.isPresent()) {
                        read++;
                    }
                }
            } finally {
                transport.close();
            }

            assertThat(read).as("every record, including those in segments served `proxy`")
                    .isEqualTo(RECORDS);
            assertThat(fetches.get())
                    .as("%d records of %d bytes cannot fit one 256 KiB inline answer, so at "
                            + "least one segment went `proxy` and was fetched", RECORDS,
                            PAD.length())
                    .isPositive();
            CrossAzBytes counted2 = node.crossAzBytes();
            assertThat(counted2.sameAzBytes(CrossAzBytes.Transport.PROXY_READ))
                    .as("the route's payload is counted, same-AZ, at least one segment's worth")
                    .isGreaterThan(256L * 1024);
        }
    }
}
