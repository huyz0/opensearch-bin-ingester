// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.ingest.RoutingPartitioner;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The routed path, through the ASSEMBLED server (M8.32, M6.19, FR-13, M8's
 * criterion 24).
 *
 * <p>⚠️ **{@code RoutedIngest} HOLDS ALL OF FR-13 AND, BEFORE THIS, EVERY ONE OF
 * ITS CONSTRUCTION SITES WAS A TEST.** The named mutation is the assembled
 * server constructing plain {@code DefaultIngest}: every explicit-partition
 * write in the tree still passes, and a routed one is a 500 -- or, had the
 * default placed rather than refused, a record in the wrong shard. So every
 * case here goes through {@link Main#run} and a real socket, and the partition
 * a routed write landed in is observed by a CONSUMER subscribed to it rather
 * than inferred from the 202.
 *
 * <p>⚠️ **THE REGISTRATION IN THE LAST CASE ARRIVES OVER HTTP**, as the
 * plugin's does. A catalog updated behind {@code RoutedIngest}'s back leaves a
 * write that is waiting for it asleep until its timeout: 503 for an index that
 * had been registered all along.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RoutedWriteTest {

    private static final String INDEX = "logs";
    private static final int RECORDS = 20;

    @TempDir
    Path dir;

    private Path configFile() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.uid=uid-pod1",
                "pod.az=az-a",
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
                    .append("{\"n\":").append(i).append("}\n");
        }
        return body.toString();
    }

    private static WebClient producer(IngesterNode node) {
        return WebClient.builder().baseUri("http://localhost:" + node.port())
                .readTimeout(Duration.ofSeconds(30)).build();
    }

    /**
     * ⚠️ A SPLIT INDEX: eight shards and sixteen routing shards, so the
     * routing factor is 2 and the answer is NOT {@code hash % numShards} -- a
     * placement that ignored the factor lands elsewhere for most values.
     */
    private static IndexRegistration split(UUID stream, int routingPartitionSize) {
        return new IndexRegistration(indexUuid(stream), INDEX, List.of(), 8, 16, 2,
                routingPartitionSize);
    }

    /** A routing value whose partition is neither 0 nor the unfactored answer. */
    private static String telling(IndexRegistration index) {
        for (int i = 0; ; i++) {
            String routing = "tenant-" + i;
            int placed = RoutingPartitioner.partitionFor(index, routing);
            int naive = Math.floorMod(RoutingPartitioner.murmur3(routing), index.numShards());
            if (placed != 0 && placed != naive) {
                return routing;
            }
        }
    }

    private static List<ConsumerRecord> readAll(IngesterNode node, RunKey key,
            Runnable write) throws Exception {
        List<ConsumerRecord> read = new ArrayList<>();
        HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                "http://localhost:" + node.port(), () -> { },
                Duration.ofMillis(50), Duration.ofSeconds(1), Duration.ofSeconds(30),
                Duration.ofSeconds(2));
        try (ConsumerClient consumer = new ConsumerClient(transport, key, 256)) {
            write.run();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (read.size() < RECORDS && System.nanoTime() < deadline) {
                Optional<ConsumerRecord> next = consumer.readNext(Duration.ofSeconds(1));
                next.ifPresent(read::add);
            }
        } finally {
            transport.close();
        }
        return read;
    }

    @Test
    void aROUTEDWriteLANDSInThePartitionOpenSearchsROUTINGChooses() throws Exception {
        UUID stream = UUID.randomUUID();
        try (IngesterNode node = Main.run(configFile().toString())) {
            IndexRegistration index = split(stream, 1);
            node.assembly().catalog().register(index);
            String routing = telling(index);
            int expected = RoutingPartitioner.partitionFor(index, routing);

            int[] status = {0};
            List<ConsumerRecord> read = readAll(node, new RunKey(stream, expected), () ->
                    status[0] = producer(node).post("/" + INDEX + "/_bulk")
                            .queryParam("routing", routing)
                            .submit(bulkBody()).status().code());

            assertThat(status[0])
                    .as("⚠️ 202: plain `DefaultIngest` has no catalog and refuses every routed "
                            + "write, which is the mutation this row exists for")
                    .isEqualTo(202);
            assertThat(read)
                    .as("⚠️ EVERY RECORD, AT PARTITION %d -- read by a consumer subscribed "
                            + "there, so a write placed anywhere else reads as nothing", expected)
                    .hasSize(RECORDS);
        }
    }

    @Test
    void anEXPLICITPartitionOUTSIDETheIndexsRangeIsREFUSEDWith400() throws Exception {
        try (IngesterNode node = Main.run(configFile().toString())) {
            node.assembly().catalog().register(split(UUID.randomUUID(), 1));

            assertThat(producer(node).post("/" + INDEX + "/_bulk").queryParam("partition", "8")
                    .submit(bulkBody()).status().code())
                    .as("⚠️ FR-13: partition 8 of an eight-shard index is a stream no shard "
                            + "polls; plain `DefaultIngest` answers 202 and loses the records")
                    .isEqualTo(400);
        }
    }

    @Test
    void aROUTEDWriteToAnIndexWithROUTINGPARTITIONSIZEOverOneIsREFUSEDWith400()
            throws Exception {
        try (IngesterNode node = Main.run(configFile().toString())) {
            node.assembly().catalog().register(split(UUID.randomUUID(), 2));

            assertThat(producer(node).post("/" + INDEX + "/_bulk")
                    .queryParam("routing", "tenant-a").submit(bulkBody()).status().code())
                    .as("⚠️ 400, NOT 500: the shard depends on the _id as well, which the "
                            + "ingester cannot compute, and the refusal is permanent")
                    .isEqualTo(400);
        }
    }

    @Test
    void aREGISTRATIONPushedOverHTTPReleasesARoutedWriteThatWasWAITINGForIt()
            throws Exception {
        UUID stream = UUID.randomUUID();
        try (IngesterNode node = Main.run(configFile().toString())) {
            IndexRegistration index = split(stream, 1);
            String routing = telling(index);
            int expected = RoutingPartitioner.partitionFor(index, routing);

            long[] took = {0};
            int[] status = {0};
            List<ConsumerRecord> read = readAll(node, new RunKey(stream, expected), () -> {
                long began = System.nanoTime();
                CompletableFuture<Integer> write = CompletableFuture.supplyAsync(() ->
                        producer(node).post("/" + INDEX + "/_bulk")
                                .queryParam("routing", routing)
                                .submit(bulkBody()).status().code());
                try {
                    // ⚠️ POLLED, NOT SLEPT (testing.md rule 15): the case is
                    // about a write that is ALREADY WAITING, and a registration
                    // that won the race would pass it without waking anything.
                    RoutedIngest routed = (RoutedIngest) node.assembly().ingest();
                    long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (routed.pendingBatches() < 1 && System.nanoTime() < waitUntil) {
                        Thread.onSpinWait();
                    }
                    assertThat(routed.pendingBatches()).as("the premise: the write is waiting")
                            .isEqualTo(1);
                    assertThat(producer(node).post(HttpSubscriptionTransport.REGISTER_PATH)
                            .submit(index.encode()).status().code()).isEqualTo(204);
                    status[0] = write.get(30, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new IllegalStateException(failed);
                }
                took[0] = System.nanoTime() - began;
            });

            assertThat(status[0])
                    .as("⚠️ 202: the registration arrived while the write waited")
                    .isEqualTo(202);
            assertThat(Duration.ofNanos(took[0]))
                    .as("⚠️ RELEASED BY THE REGISTRATION, NOT BY A POLL: a write left asleep "
                            + "until the pending timeout of 5 s is the catalog updated behind "
                            + "the routed path's back")
                    .isLessThan(Duration.ofSeconds(3));
            assertThat(read).hasSize(RECORDS);
        }
    }
}
