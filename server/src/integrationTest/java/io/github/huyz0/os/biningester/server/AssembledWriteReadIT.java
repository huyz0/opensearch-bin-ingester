// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.backend.MinioFixture;
import io.github.huyz0.os.biningester.binstore.backend.S3BinStore;
import io.github.huyz0.os.biningester.binstore.backend.S3Settings;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.helidon.webclient.api.WebClient;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * M8's criterion 1: the assembled process accepts a write and a consumer reads
 * it back, against a real S3-compatible endpoint (M8.4, FR-1, FR-13).
 *
 * <p>⚠️ **THE STORE ASSERTION IS THE LOAD-BEARING ONE, AND THE SPEC SAYS SO.**
 * A process that acked out of its accumulator and answered the read-back out of
 * the same JVM's memory is GREEN on the 202 and green on the records — that is
 * criterion 1's named mutation, and neither the request nor the reply can see
 * it. So the bytes are fetched from the bucket by a SECOND, independently-opened
 * client that shares nothing with the node but the endpoint, and the key they
 * were found under is parsed against {@link SegmentKey}'s grammar.
 *
 * <p>⚠️ **THE CREDENTIAL REACHES THE NODE THROUGH THE ENVIRONMENT, NEVER
 * THROUGH THE CONFIG FILE.** {@code StoreFactory} opens {@code s3} with no
 * credentials argument, so the SDK's default provider chain resolves them —
 * here from system properties, in a deployment from the container's role. That
 * is the shape security.md rule 5 requires and it is asserted by construction:
 * the file below has no secret in it and the node still reaches the bucket.
 *
 * <p>⚠️ **AND THE CONSUMER IS A REAL ONE OVER A REAL SOCKET**, through
 * {@code HttpSubscriptionTransport} against the node's own subscription
 * service — the seam that existed only as a {@code testFixtures} fake until
 * M8.21. What it demonstrates is the END of the walking skeleton: producer's
 * HTTP request, durable segment in a bucket, subscription push, decoded record.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AssembledWriteReadIT {

    private static final String INDEX = "logs";
    private static final int PARTITION = 3;
    private static final int RECORDS = 100;
    private static final String PREFIX = "bins/cluster-a";

    private static String endpoint;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        endpoint = MinioFixture.endpoint();
    }

    private static String makeBucket() {
        String bucket = "m84-" + UUID.randomUUID();
        try (S3Client admin = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        MinioFixture.ACCESS_KEY, MinioFixture.SECRET_KEY)))
                .build()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
        return bucket;
    }

    /**
     * A second client onto the same bucket, sharing NOTHING with the node.
     *
     * <p>⚠️ **THIS IS THE INSTRUMENT.** Reading back through
     * {@code node.assembly().store()} would ask the process under test whether
     * it had written, which is the question.
     */
    private static BinStore observer(String bucket) {
        return S3BinStore.open(new S3Settings(endpoint, "us-east-1", bucket, true,
                255 /* MinIO's measured limit, M8.22 */),
                StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        MinioFixture.ACCESS_KEY, MinioFixture.SECRET_KEY)));
    }

    private Path configFile(String bucket) throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=" + PREFIX,
                "store.kind=s3",
                "store.endpoint=" + endpoint,
                "store.region=us-east-1",
                "store.bucket=" + bucket,
                "store.path-style=true",
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=" + INDEX,
                // ⚠️ SHORT, so the first flush does not cost this test its
                // patience. It is the adaptive interval's FLOOR; the ceiling
                // and the fill-ratio policy are M3's and are not touched here.
                "ingest.interval-floor=PT0.05S",
                "").getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String indexUuid(UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    private static String bulkBody() {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < RECORDS; i++) {
            body.append("{\"index\":{\"_id\":\"doc-").append(i).append("\",\"_version\":1}}\n")
                    .append("{\"n\":").append(i).append("}\n");
        }
        return body.toString();
    }

    @Test
    void theASSEMBLEDProcessACCEPTSAWriteAndAConsumerREADSItBackFromTheBUCKET()
            throws Exception {
        String bucket = makeBucket();
        // ⚠️ THE SDK's DEFAULT CHAIN READS THESE. The config file carries no
        // secret and `StoreFactory` passes no credentials, so if this is the
        // only place a credential appears then the production path is the one
        // resolving it.
        System.setProperty("aws.accessKeyId", MinioFixture.ACCESS_KEY);
        System.setProperty("aws.secretAccessKey", MinioFixture.SECRET_KEY);

        UUID stream = UUID.randomUUID();
        try (IngesterNode node = Main.run(configFile(bucket).toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    indexUuid(stream), INDEX, List.of(), 8, 8, 1, 1));

            RunKey key = new RunKey(stream, PARTITION);
            List<ConsumerRecord> read = new ArrayList<>();
            // ⚠️ NOT A try-WITH-RESOURCES. `HttpSubscriptionTransport` has a
            // `close()` and does not implement `AutoCloseable` -- the seam it
            // implements declares closing on the SUBSCRIPTION, not on the
            // transport -- so this is the honest shape rather than a cast.
            HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                    "http://localhost:" + node.port(), () -> { },
                    Duration.ofMillis(50), Duration.ofSeconds(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(2));
            try (ConsumerClient consumer = new ConsumerClient(transport, key, 256)) {

                // ⚠️ THE CONSUMER SUBSCRIBES BEFORE THE WRITE. A subscription
                // opened afterwards would be asserting the resume path, which
                // is M8.18's and M8.31's; what is claimed here is the LIVE
                // tail.
                WebClient producer = WebClient.builder()
                        .baseUri("http://localhost:" + node.port()).build();
                assertThat(producer.post("/" + INDEX + "/_bulk")
                        .queryParam("partition", String.valueOf(PARTITION))
                        .submit(bulkBody()).status().code())
                        .as("⚠️ 202 ONLY AFTER DURABLE: `append` returns once the segment AND "
                                + "its commit delta are in the bucket")
                        .isEqualTo(202);

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                while (read.size() < RECORDS && System.nanoTime() < deadline) {
                    Optional<ConsumerRecord> next = consumer.readNext(Duration.ofSeconds(5));
                    next.ifPresent(read::add);
                }
            } finally {
                transport.close();
            }

            // ⚠️ EVERY RECORD, IN ORDER, AND THE PAYLOADS ARE THE REQUEST'S
            // OWN. A count alone is satisfied by a handler that passed the
            // first record a hundred times.
            assertThat(read).hasSize(RECORDS);
            for (int i = 0; i < RECORDS; i++) {
                ConsumerRecord got = read.get(i);
                assertThat(got.record().id())
                        .as("⚠️ IN THE REQUEST's ORDER, record %d", i)
                        .isEqualTo("doc-" + i);
                assertThat(new String(got.payload(), StandardCharsets.UTF_8))
                        .as("⚠️ AND THE PAYLOAD IS THIS RECORD's, not the first one repeated")
                        .contains("\"_source\":{\"n\":" + i + "}");
                if (i > 0) {
                    assertThat(got.offset())
                            .as("⚠️ CONTIGUOUS OFFSETS. A gap here is a consumer that would "
                                    + "never learn what it missed")
                            .isEqualTo(read.get(i - 1).offset() + 1);
                }
            }
        } finally {
            System.clearProperty("aws.accessKeyId");
            System.clearProperty("aws.secretAccessKey");
        }

        // ⚠️ AND NOW THE QUESTION THE PROCESS CANNOT BE ASKED: is the object in
        // the BUCKET? The node is closed; this client was never part of it.
        try (BinStore observer = observer(bucket)) {
            ListPage page = observer.list(PREFIX + "/data/", null, 1000);
            assertThat(page.objects())
                    .as("⚠️ AT THE STORE. A process that acked from memory is red HERE and "
                            + "nowhere else -- the 202 and the read-back above are both green "
                            + "for it")
                    .isNotEmpty();

            String segmentKey = page.objects().get(0).key();
            // ⚠️ THE GRAMMAR, READ BY THE CLASS THAT OWNS IT. `timestampOf` and
            // `headerLenOf` each throw "not a segment key" on anything that
            // does not match, so the two calls ARE the assertion -- and the
            // hour path is derived from the timestamp they returned, so a key
            // filed under the wrong hour does not match its own name. A regex
            // written here would be a second copy of the grammar, and the copy
            // is what goes stale.
            long timestampMillis = SegmentKey.timestampOf(segmentKey);
            assertThat(SegmentKey.headerLenOf(segmentKey)).isPositive();
            assertThat(segmentKey)
                    .as("⚠️ UNDER THE CONFIGURED PREFIX AND THE HOUR ITS OWN TIMESTAMP NAMES")
                    .startsWith(SegmentKey.hourPrefix(PREFIX, timestampMillis))
                    .endsWith(".bseg");

            byte[] segment;
            try (InputStream in = observer.get(segmentKey)) {
                segment = in.readAllBytes();
            }
            assertThat(SegmentReader.open(segment).directory())
                    .as("⚠️ THE BYTES IN THE BUCKET CARRY THIS RUN AND ALL ITS RECORDS -- "
                            + "partition " + PARTITION + ", not a constant one")
                    .anyMatch(entry -> entry.key().equals(new RunKey(stream, PARTITION))
                            && entry.recordCount() == RECORDS);

            assertThat(observer.list(PREFIX + "/ctl/log/", null, 100).objects())
                    .as("⚠️ AND THE COMMIT LOG IS IN THE SAME BUCKET UNDER THE SAME PREFIX. "
                            + "A sequencer given a different store commits offsets nothing "
                            + "ever reads")
                    .isNotEmpty();
        }
    }
}
