// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.ConsumerClient;
import binjava.client.HttpSubscriptionTransport;
import binjava.client.PositionCollectedException;
import binjava.format.Checkpoint;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import binjava.sequencer.Checkpoints;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.LocalSequencer;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * GC deletes, the floor travels, and the consumer refuses -- one test, the
 * assembled node, a real socket (M8.30, M7.23, FR-9, FR-10, M8's criterion 22).
 *
 * <p>⚠️ **M7 ASSERTED THE TWO HALVES SEPARATELY**, because {@code ingest} does
 * not depend on {@code client}: the boundary was REPORTED in one test and the
 * refusal asserted against a floor set BY HAND in another, so both stayed green
 * while nothing carried the floor between them. Here nothing is set by hand:
 * the segments are deleted by the node's own retention loop, the floor is the
 * one its own checkpoint recorded, and it reaches the consumer over HTTP.
 *
 * <p>⚠️ **T1, NOT THE T4 {@code RetentionRefusalIT} M7's PLAN NAMED**: nothing
 * in the join needs a container, and the store the node owns is the one the
 * pass deletes from. The name is the plan's; the tier is testing.md rule 1's.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RetentionRefusalTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final int RECORDS = 20;

    @TempDir
    Path dir;

    private Path configFile() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=" + PREFIX,
                "store.kind=memory",
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                "ingest.interval-floor=PT0.05S",
                // ⚠️ A CEILING OF ONE SECOND, so the pass deletes by AGE with no
                // consumer watermark involved: the join, not the policy.
                "retention.min=PT0.5S",
                "retention.max=PT1S",
                "retention.report-timeout=PT0.1S",
                "retention.copy-expiry=PT2S",
                "retention.pass-interval=PT0.2S",
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

    private static void await(String what, java.util.function.BooleanSupplier done)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!done.getAsBoolean()) {
            assertThat(System.nanoTime()).as("never: %s", what).isLessThan(deadline);
            Thread.sleep(50);
        }
    }

    @Test
    void aPOSITIONTheNodesOWNGCCollectedIsREFUSEDByAConsumerOverHTTP() throws Exception {
        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        try (IngesterNode node = Main.run(configFile().toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    indexUuid(index), "logs", List.of(), 4, 4, 1, 1));
            assertThat(WebClient.builder().baseUri("http://localhost:" + node.port()).build()
                    .post("/logs/_bulk").queryParam("partition", "0").submit(bulkBody())
                    .status().code()).isEqualTo(202);

            var store = node.assembly().store();
            await("the node's own retention pass deleted the segment", () -> {
                try {
                    return store.list(PREFIX + "/data/", null, 100).objects().isEmpty();
                } catch (java.io.IOException failed) {
                    throw new java.io.UncheckedIOException(failed);
                }
            });

            // ⚠️ THE CHECKPOINT IS TRIGGERED, NOT FORGED: K commits on ANOTHER
            // stream make the term's own writer checkpoint on its delta count,
            // and what it writes for `stream` is what the pass observed.
            LocalSequencer local = LocalSequencer.underneath(node.assembly().heldTerm())
                    .orElseThrow();
            RunKey other = new RunKey(UUID.randomUUID(), 0);
            for (long i = 0; i < LocalSequencer.CHECKPOINT_EVERY_DELTAS; i++) {
                local.commit(new CommitRequest("p", "i", i, "absent/" + i, Map.of(other, 1)));
            }
            long epoch = local.epoch();
            await("a checkpoint recording the collected floor", () -> {
                try {
                    Optional<Checkpoint> newest = Checkpoints.newest(store, PREFIX, epoch);
                    return newest.map(c -> c.streams().get(stream))
                            .map(o -> o.oldestRetainedOffset() == RECORDS).orElse(false);
                } catch (java.io.IOException failed) {
                    throw new java.io.UncheckedIOException(failed);
                }
            });

            HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                    "http://localhost:" + node.port(), () -> { },
                    Duration.ofMillis(50), Duration.ofSeconds(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(1));
            try (ConsumerClient consumer = new ConsumerClient(transport, stream, 64)) {
                consumer.requestFreshFloor();
                await("the floor reached the consumer over HTTP",
                        () -> consumer.retainedFloor().isPresent());

                assertThatThrownBy(() -> consumer.refuseIfCollected(0))
                        .as("⚠️ OFFSET 0 WAS DELETED BY THE PASS ABOVE: the consumer must "
                                + "say so, naming the range, rather than wait for records that "
                                + "will never arrive")
                        .isInstanceOfSatisfying(PositionCollectedException.class, refused -> {
                            assertThat(refused.key()).isEqualTo(stream);
                            assertThat(refused.oldestRetainedOffset()).isEqualTo(RECORDS);
                            assertThat(refused.lostRecords()).isEqualTo(RECORDS);
                        });
                consumer.refuseIfCollected(RECORDS);
            } finally {
                transport.close();
            }
        }
    }
}
