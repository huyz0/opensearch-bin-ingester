// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.binstore.backend.S3BinStore;
import io.github.huyz0.os.biningester.binstore.backend.S3Settings;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * A fresh RustFS bucket for one chaos row, and the instrument that reads it
 * (M8.8).
 *
 * <p>⚠️ **EVERY ASSERTION IS MADE HERE, AT THE STORE, THROUGH A CLIENT THAT
 * SHARES NOTHING WITH THE NODES.** A node asked whether it wrote is being
 * asked the question under test, and a node that has been killed cannot be
 * asked at all. The bucket is the one witness that outlives every process.
 *
 * <p>⚠️ **RustFS, NOT LOCAL-FS.** The spec rejects running chaos on local-FS
 * (ADR-0052 § Alternatives): a rename on a local disk is not a conditional
 * PUT, so a row about a lease race would be testing a different store.
 */
public final class ChaosBucket implements AutoCloseable, ChainAudit.ChainBucketReader {

    /** The prefix every node in a chaos row writes under. */
    public static final String PREFIX = "bins/cluster-a";

    private static final Pattern EXPIRY = Pattern.compile("\"expiresAtMillis\":(\\d+)");

    private final String endpoint;
    private final String name;
    private final CountingBinStore observer;

    private ChaosBucket(String endpoint, String name) {
        this.endpoint = endpoint;
        this.name = name;
        this.observer = new CountingBinStore(S3BinStore.open(
                new S3Settings(endpoint, "us-east-1", name, true, 255),
                StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY))));
    }

    /** Makes a new bucket on the shared RustFS container. */
    public static ChaosBucket create() {
        String endpoint = S3Fixture.endpoint();
        String name = "chaos-" + UUID.randomUUID();
        try (S3Client admin = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY)))
                .build()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(name).build());
        }
        return new ChaosBucket(endpoint, name);
    }

    /**
     * The settings a node needs to write to this bucket.
     *
     * <p>⚠️ **NO CREDENTIAL IS IN THEM.** A node reads its credentials from the
     * SDK's default chain, and {@link NodeProcess} passes them in the
     * environment, as a pod would receive them.
     */
    public Map<String, String> nodeSettings() {
        return Map.of(
                "store.prefix", PREFIX,
                "store.kind", "s3",
                "store.endpoint", endpoint,
                "store.region", "us-east-1",
                "store.bucket", name,
                "store.path-style", "true");
    }

    /** The environment a node process reads its credentials from. */
    public static Map<String, String> credentials() {
        return Map.of("AWS_ACCESS_KEY_ID", S3Fixture.ACCESS_KEY,
                "AWS_SECRET_ACCESS_KEY", S3Fixture.SECRET_KEY,
                "AWS_REGION", "us-east-1");
    }

    /** The store, read by nobody but the test. */
    public BinStore observer() {
        return observer;
    }

    /** The object-store requests made by the independent test observer. */
    public io.github.huyz0.os.biningester.binstore.StoreCounts observerCounts() {
        return observer.counts();
    }

    /** Every key under {@code prefix}, following every page. */
    @Override
    public List<String> keys(String prefix) throws java.io.IOException {
        List<String> keys = new ArrayList<>();
        String after = null;
        while (true) {
            var page = observer.list(prefix, after, 1000);
            for (ObjectStat object : page.objects()) {
                keys.add(object.key());
            }
            if (page.nextStartAfter().isEmpty()) {
                return keys;
            }
            after = page.nextStartAfter().get();
        }
    }

    /** One LIST page, refusing to hide extra requests behind pagination. */
    public List<String> keysOnePage(String prefix) throws java.io.IOException {
        var page = observer.list(prefix, null, 1000);
        if (page.nextStartAfter().isPresent()) {
            throw new java.io.IOException("one-page observation exceeded 1000 keys: " + prefix);
        }
        return page.objects().stream().map(ObjectStat::key).toList();
    }

    /** One object's bytes. */
    @Override
    public byte[] get(String key) throws java.io.IOException {
        try (var in = observer.get(key)) {
            return in.readAllBytes();
        }
    }

    /** The lease object, decoded: who holds the term, and until when. */
    public java.util.Optional<io.github.huyz0.os.biningester.format.Lease> lease() throws java.io.IOException {
        List<String> lease = keys(PREFIX + "/ctl/lease/");
        if (lease.isEmpty()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(io.github.huyz0.os.biningester.format.Lease.decode(get(lease.get(0))));
    }

    /** The segments written so far. */
    public List<String> segments() throws java.io.IOException {
        return keys(PREFIX + "/data/");
    }

    /**
     * When the sequencer lease says it expires, or empty if no node has ever
     * held it.
     *
     * <p>⚠️ **THE EXPIRY, NOT THE HOLDER.** A release moves the expiry into
     * the past and keeps the name, which is how a successor knows whom it
     * replaced; renewal moves it forward. Both are read off this one number.
     */
    public java.util.OptionalLong leaseExpiry() throws java.io.IOException {
        List<String> lease = keys(PREFIX + "/ctl/lease/");
        if (lease.isEmpty()) {
            return java.util.OptionalLong.empty();
        }
        String text;
        try (var in = observer.get(lease.get(0))) {
            text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        Matcher matcher = EXPIRY.matcher(text);
        if (!matcher.find()) {
            throw new AssertionError("a lease object with no expiry: " + text);
        }
        return java.util.OptionalLong.of(Long.parseLong(matcher.group(1)));
    }

    @Override
    public void close() throws java.io.IOException {
        observer.close();
    }
}
