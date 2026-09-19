// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.CostTable;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.SignedUrl;
import binjava.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The first real object-store backend: S3, and anything that speaks it (M8.2,
 * NFR-8, ADR-0052, ADR-0054).
 *
 * <p>⚠️ **THIS IS WHERE M8's EVIDENCE BECOMES MEASURABLE RATHER THAN MODELLED.**
 * The two backends that came before it are a map in the caller's own heap and a
 * directory: both answer {@code presignedUrls=false}, neither has a network, and
 * neither can be partitioned away from a pod. ADR-0052 is the argument for why
 * this lands before any chaos task rather than after the milestone that breaks
 * things.
 *
 * <p>⚠️ **EVERY VENDOR EXCEPTION IS TRANSLATED AT THIS BOUNDARY.** The SDK's
 * failures are {@code RuntimeException}s; the SPI declares {@code IOException}
 * on every I/O method precisely so a caller can retry. One escaping
 * {@code S3Exception} unwinds the commit-log retry loop (ADR-0002) instead of
 * letting it re-read and try the next sequence number — a lost write that looks
 * like a crash.
 *
 * <p>⚠️ **THE TWO CONDITIONAL WRITES ARE HTTP PRECONDITIONS, NOT A TRANSACTION.**
 * {@code putIfAbsent} is {@code If-None-Match: *} and {@code putIfMatch} is
 * {@code If-Match: <etag>}; both answer 412 when they lose. ⚠️ **AND THE TWO
 * FAILURES ARE NOT THE SAME FAILURE**: a 412 is "someone else got there", which
 * is an ordinary empty; a 404 under {@code If-Match} is "there is nothing to
 * match", which throws, because a lease renewal that read its own lease's
 * DISAPPEARANCE as "another pod holds it" would wait out a TTL for a term
 * nobody holds.
 *
 * <p>⚠️ **NO REQUEST HERE SCALES WITH RECORDS, SHARDS, PARTITIONS OR INDICES**
 * (non-negotiable 6). One call on this class is one request to the endpoint:
 * {@code list} takes a page size and answers one page rather than hiding paging
 * inside the backend, which is the SPI's own reason for the signature, and
 * {@code delete} is one batched request for up to {@value #MAX_DELETE_BATCH}
 * keys.
 */
public final class S3BinStore implements BinStore {


    /** ⚠️ 5 MiB: S3's own minimum for every part but the last. */
    private static final long MIN_PART_SIZE = 5L * 1024 * 1024;

    /** ⚠️ 1,000 keys, the maximum one {@code DeleteObjects} request accepts. */
    static final int MAX_DELETE_BATCH = 1000;

    /**
     * ⚠️ **THE TWO CONDITIONAL WRITES DO NOT RETRY, AND EVERYTHING ELSE DOES.**
     * A retried conditional write is a correctness hazard rather than a slow
     * path: a {@code putIfAbsent} whose first attempt SUCCEEDED and whose
     * response was lost answers 412 on the retry, which this class reports as
     * "someone else got there" — a writer that won the race being told it lost,
     * in the one place the commit log cannot tell the difference (ADR-0002).
     * The retry that belongs there is the one that re-reads the chain.
     *
     * <p>⚠️ **PER REQUEST, NOT ON THE CLIENT.** An earlier draft set this on the
     * builder, which also disabled retries for {@code get}, {@code getRange},
     * {@code stat}, {@code list}, {@code delete} and every multipart part — all
     * of them idempotent, all of them worth retrying, and the endpoint dropping
     * connections is a thing this very commit measures. A plugin is how the SDK
     * scopes configuration to one call.
     */
    private static final software.amazon.awssdk.core.SdkPlugin NO_RETRY = config ->
            config.overrideConfiguration(config.overrideConfiguration().toBuilder()
                    .retryStrategy(software.amazon.awssdk.retries.DefaultRetryStrategy.doNotRetry())
                    .build());

    private final S3Client client;
    private final String bucket;

    /**
     * ⚠️ **THE ENDPOINT'S LIMIT, NOT S3's, AND M8.22 IS WHY.** Running the
     * shared conformance suite against MinIO refused a 1,024-byte key that the
     * capability had advertised as acceptable: MinIO's limit is 255 BYTES per
     * path component. A capability is a promise a deployment checks at startup,
     * so it has to describe the endpoint in front of it.
     */
    private final long maxKeyBytes;

    /**
     * ⚠️ **THE PROVIDER THIS STORE BUILT, OR NULL WHERE THE CALLER SUPPLIED
     * ONE.** `DefaultCredentialsProvider` holds a background refresh, and a
     * user-supplied provider is NOT closed by the SDK — so a pod that stopped
     * its store would go on refreshing credentials for the life of the JVM.
     * ⚠️ A CALLER'S PROVIDER IS NEVER CLOSED HERE: the chaos harness builds one
     * and hands it to several stores, and closing it with the first of them
     * would break the rest.
     */
    private final AutoCloseable ownedCredentials;

    /**
     * ⚠️ **SIGNS LOCALLY, FROM THE SAME CREDENTIAL AS THE CLIENT** (M8.18). SigV4
     * is an HMAC over the request, so a grant costs no request; or NULL, where
     * a test built the store around a client of its own, and then this store
     * says it cannot presign rather than signing with something else.
     */
    private final S3Presigner presigner;

    /**
     * Opens a store over the endpoint's default credential chain.
     *
     * <p>⚠️ **`builder().build()`, NOT THE DEPRECATED `create()`.** They differ
     * in more than spelling: the builder's instance is one this store CLOSES,
     * while `create()`'s is a shared singleton whose background refresh outlives
     * it — a pod that stopped its store would keep refreshing credentials.
     */
    public static S3BinStore open(S3Settings settings) {
        DefaultCredentialsProvider credentials = DefaultCredentialsProvider.builder().build();
        try {
            return new S3BinStore(build(settings, credentials), settings.bucket(), credentials,
                    settings.maxKeyBytes(), presigner(settings, credentials));
        } catch (RuntimeException failed) {
            credentials.close();
            throw failed;
        }
    }

    /**
     * Opens a store with the credentials given.
     *
     * <p>⚠️ **AN EXPLICIT PROVIDER IS A CALL SITE A REVIEWER CAN GREP FOR.** A
     * deployment uses the overload above and gives the pod an identity; this one
     * exists for a test fixture and for the deployment that genuinely has a
     * static pair, and naming it in the signature is what keeps that visible.
     */
    public static S3BinStore open(S3Settings settings, AwsCredentialsProvider credentials) {
        return new S3BinStore(build(settings, credentials), settings.bucket(), null,
                settings.maxKeyBytes(), presigner(settings, credentials));
    }

    /** A presigner for the same endpoint, region, path style and credential as the client. */
    static S3Presigner presigner(S3Settings settings, AwsCredentialsProvider credentials) {
        var builder = S3Presigner.builder()
                .region(Region.of(settings.region()))
                .credentialsProvider(credentials)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(settings.pathStyle()).build());
        if (settings.endpoint() != null && !settings.endpoint().isBlank()) {
            builder.endpointOverride(URI.create(settings.endpoint()));
        }
        return builder.build();
    }

    private static S3Client build(S3Settings settings, AwsCredentialsProvider credentials) {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(credentials, "credentials");
        var builder = S3Client.builder()
                .region(Region.of(settings.region()))
                .credentialsProvider(credentials)
                // ⚠️ THE JDK's OWN HTTP CLIENT. Every call this class makes is
                // blocking on a virtual thread (research 40-01 §1), so the SDK
                // needs neither Netty nor a reactive runtime -- and both are
                // excluded from the build, which is why naming this is not
                // optional: without it the SDK looks for a client on the
                // classpath and fails at construction rather than at first use.
                // ⚠️ `httpClientBuilder`, NOT `httpClient`: a BUILT client is
                // the caller's and the SDK never closes it, so `client.close()`
                // would leave its connection pool and its threads behind. Handed
                // the builder, the SDK owns the instance and closes it with the
                // client.
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .forcePathStyle(settings.pathStyle());
        if (settings.endpoint() != null && !settings.endpoint().isBlank()) {
            builder.endpointOverride(URI.create(settings.endpoint()));
        }
        return builder.build();
    }

    S3BinStore(S3Client client, String bucket) {
        this(client, bucket, null, S3Settings.S3_MAX_KEY_BYTES);
    }

    /**
     * ⚠️ **PACKAGE-PRIVATE SO THE OWNERSHIP CAN BE ASSERTED.** Whether this
     * store closes what it built is invisible from outside it — review measured
     * the whole close path removable with both suites green — and a case needs
     * to hand in a closeable it can watch.
     */
    S3BinStore(S3Client client, String bucket, AutoCloseable ownedCredentials, long maxKeyBytes) {
        this(client, bucket, ownedCredentials, maxKeyBytes, null);
    }

    S3BinStore(S3Client client, String bucket, AutoCloseable ownedCredentials, long maxKeyBytes,
            S3Presigner presigner) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.ownedCredentials = ownedCredentials;
        this.maxKeyBytes = maxKeyBytes;
        this.presigner = presigner;
    }

    @Override
    public InputStream get(String key) throws IOException {
        return translating(key, () -> client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build()));
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        if (start < 0 || endIncl < start) {
            // ⚠️ AN `IOException`, NOT AN `IllegalArgumentException`. The shared
            // conformance suite asserts this type for every backend, and M8.22
            // measured this backend answering the other one -- a caller
            // catching what the SPI declares would have been unwound instead.
            throw new IOException("not a range: [" + start + ", " + endIncl + "]");
        }
        // ⚠️ HTTP's RANGE IS INCLUSIVE AT BOTH ENDS AND SO IS THE SPI's, which
        // is the only reason this is a straight copy rather than a conversion.
        // An exclusive end here is off by one on every fetch and the bytes still
        // PARSE, because a record boundary lands inside the segment.
        return translating(key, () -> client.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(key)
                .range("bytes=" + start + "-" + endIncl)
                .build()));
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        try {
            HeadObjectResponse head = client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return Optional.of(new ObjectStat(key, head.contentLength(),
                    new Version(head.eTag())));
        } catch (NoSuchKeyException absent) {
            return Optional.empty();
        } catch (S3Exception failed) {
            // ⚠️ A HEAD ANSWERS 404 WITH NO BODY, so an endpoint whose error
            // code the SDK cannot read raises a plain `S3Exception` rather than
            // `NoSuchKeyException`. ⚠️ **UNREACHED AGAINST MinIO, MEASURED**:
            // review deleted this branch and the whole T3 suite stayed green,
            // because MinIO's HEAD is mapped to `NoSuchKeyException` above. It
            // is kept for the endpoints that do not, and is stated as
            // unmeasured rather than presented as the mechanism.
            if (failed.statusCode() == 404) {
                return Optional.empty();
            }
            throw asIoException("stat " + key, failed);
        } catch (SdkException failed) {
            throw asIoException("stat " + key, failed);
        }
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        PutObjectResponse written = translating("put " + key, () -> client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(), requestBody(body)));
        return new Version(written.eTag());
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        if (refusedBeforeAnyRequest(key, body, null)) {
            return Optional.empty();
        }
        try {
            PutObjectResponse written = client.putObject(PutObjectRequest.builder()
                    .bucket(bucket).key(key)
                    // ⚠️ `If-None-Match: *` -- write only if nothing is there.
                    // This is ADR-0011's CAS and the whole commit log rests on
                    // it; a backend without it cannot serve this project at all,
                    // which `Capabilities.requireConditionalWrites()` refuses at
                    // startup rather than at the first contended commit.
                    .ifNoneMatch("*")
                    .overrideConfiguration(o -> o.addPlugin(NO_RETRY))
                    .build(), validatedBody(body));
            return Optional.of(new Version(written.eTag()));
        } catch (S3Exception lost) {
            if (isPreconditionFailed(lost)) {
                // ⚠️ EMPTY MEANS ALREADY EXISTS, and it is NORMAL. The commit
                // log is a chain of these (ADR-0002): a loser re-reads and
                // retries at the next sequence number. A throw here would make
                // every contended commit an error.
                return Optional.empty();
            }
            throw asIoException("putIfAbsent " + key, lost);
        } catch (SdkException | java.io.UncheckedIOException failed) {
            throw asIoException("putIfAbsent " + key, failed);
        }
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        Objects.requireNonNull(expected, "expected");
        if (refusedBeforeAnyRequest(key, body, expected)) {
            return Optional.empty();
        }
        try {
            PutObjectResponse written = client.putObject(PutObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .ifMatch(expected.token())
                    .overrideConfiguration(o -> o.addPlugin(NO_RETRY))
                    .build(), validatedBody(body));
            return Optional.of(new Version(written.eTag()));
        } catch (S3Exception lost) {
            if (isNotFound(lost)) {
                // ⚠️ AN ABSENT KEY IS A DIFFERENT FAILURE AND IS NOT FOLDED
                // INTO THE EMPTY CASE: there is no version to have moved from.
                // A lease renewal that read its own lease's DISAPPEARANCE as
                // "another pod holds it" would wait out a TTL for a term nobody
                // holds. ⚠️ THE TWO ARE TOLD APART BY STATUS AND NOT BY ORDER:
                // 404 and 412 are one value each, so writing this branch second
                // would behave identically. An earlier draft of this comment
                // claimed the order was load-bearing, which review measured as
                // false by swapping the blocks.
                throw asIoException("putIfMatch " + key + ": there is no object to match "
                        + "against, so no version has moved", lost);
            }
            if (isPreconditionFailed(lost)) {
                // ⚠️ EMPTY MEANS THE VERSION MOVED (ADR-0008) -- the losing side
                // of a lease renewal or an ordinal-registry update re-reads and
                // decides whether to retry.
                return Optional.empty();
            }
            throw asIoException("putIfMatch " + key, lost);
        } catch (SdkException | java.io.UncheckedIOException failed) {
            throw asIoException("putIfMatch " + key, failed);
        }
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        return S3MultipartWriter.begin(client, bucket, key, MIN_PART_SIZE);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys is positive: " + maxKeys);
        }
        var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(maxKeys);
        if (startAfter != null) {
            // ⚠️ STRICTLY AFTER. A resume that re-read its own cursor would
            // deliver one object twice at every page boundary -- which a GC pass
            // would count twice and a recovery walk would replay.
            request.startAfter(startAfter);
        }
        ListObjectsV2Response page = translating("list " + prefix,
                () -> client.listObjectsV2(request.build()));
        List<ObjectStat> objects = new ArrayList<>(page.contents().size());
        for (S3Object object : page.contents()) {
            objects.add(new ObjectStat(object.key(), object.size(), new Version(object.eTag())));
        }
        // ⚠️ THE CURSOR IS THE LAST KEY, NOT THE ENDPOINT'S CONTINUATION TOKEN.
        // The SPI's resume is a KEY, so a caller can restart a walk from
        // somewhere it recorded days ago; a continuation token is opaque and
        // expires. ⚠️ AND `isTruncated` IS WHAT SAYS THERE IS MORE: a full page
        // that happens to be the last one would otherwise loop for ever, asking
        // for what comes after the final key.
        if (Boolean.TRUE.equals(page.isTruncated()) && objects.isEmpty()) {
            // ⚠️ TRUNCATED WITH NOTHING IN IT IS NOT AN END OF WALK. Answering
            // an empty cursor here would stop a recovery walk or a GC pass
            // silently, mid-prefix, and everything after that point would look
            // as though it did not exist. There is no key to resume from, so the
            // only honest answer is to refuse.
            throw new IOException("list " + prefix + " answered a truncated page with no "
                    + "objects, so there is no key to resume from");
        }
        Optional<String> next = Boolean.TRUE.equals(page.isTruncated())
                ? Optional.of(objects.get(objects.size() - 1).key())
                : Optional.empty();
        return new ListPage(objects, next);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        Objects.requireNonNull(keys, "keys");
        // ⚠️ NO REQUEST AT ALL FOR AN EMPTY LIST, and that is this loop's
        // CONDITION rather than a guard in front of it. An earlier draft had
        // both; review's mutation showed the guard was dead -- with it deleted,
        // zero iterations still meant zero requests -- and dead code that
        // carries the argument for a property is worse than none, because the
        // next reader moves it. The property itself is NFR-2's: a GC pass with
        // nothing to delete would otherwise cost one request per pass per
        // prefix, for ever, on a deployment doing nothing.
        for (int from = 0; from < keys.size(); from += MAX_DELETE_BATCH) {
            List<String> batch = keys.subList(from, Math.min(keys.size(), from + MAX_DELETE_BATCH));
            List<ObjectIdentifier> identifiers = new ArrayList<>(batch.size());
            for (String key : batch) {
                identifiers.add(ObjectIdentifier.builder().key(key).build());
            }
            DeleteObjectsResponse answer = translating("delete " + batch.size() + " keys",
                    () -> client.deleteObjects(DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(identifiers).quiet(true).build())
                            .build()));
            // ⚠️ A BATCH DELETE ANSWERS 200 WITH PER-KEY FAILURES INSIDE IT.
            // The SDK does not throw for those, so a store that ignored this
            // body would report a successful GC pass having deleted nothing --
            // and the retention loop would move its floor past objects that are
            // still there and still billed. ⚠️ AN ABSENT KEY IS NOT ONE OF
            // THEM: S3 reports deleting a key that was never there as success,
            // which the SPI requires.
            if (!answer.errors().isEmpty()) {
                var first = answer.errors().get(0);
                throw new IOException("delete refused " + answer.errors().size() + " of "
                        + batch.size() + " keys; first was " + first.key() + ": "
                        + first.code());
            }
        }
    }

    /**
     * A GET grant on one key, signed locally (M8.18, ADR-0041).
     *
     * <p>⚠️ **A FAILURE CARRIES NEITHER ITS MESSAGE NOR ITS CAUSE** (security.md
     * rule 4, M5.42). The SDK's own text for a credential failure can name the
     * key, the source it was fetched from, or a partly-built URL, and a cause
     * travels with every printed trace. Only the failure's TYPE is kept.
     */
    @Override
    public SignedUrl presign(String key, Duration ttl) throws IOException {
        if (presigner == null) {
            return BinStore.super.presign(key, ttl);
        }
        Objects.requireNonNull(key, "key");
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("a grant's TTL is positive: " + ttl);
        }
        PresignedGetObjectRequest signed;
        try {
            signed = presigner.presignGetObject(GetObjectPresignRequest.builder()
                    .signatureDuration(ttl)
                    .getObjectRequest(GetObjectRequest.builder().bucket(bucket).key(key).build())
                    .build());
        } catch (RuntimeException failed) {
            throw new IOException("presigning " + key + " failed ("
                    + failed.getClass().getSimpleName() + "); the detail is withheld because it "
                    + "can carry a credential");
        }
        return new SignedUrl(signed.url().toString(), signed.expiration());
    }

    @Override
    public Capabilities capabilities() {
        // ⚠️ `presignedUrls` IS WHETHER THIS STORE HAS A PRESIGNER, and saying so
        // is the whole mechanism: a deployment that wants `direct` calls
        // `requirePresignedUrls()` at startup and is refused, rather than
        // discovering at the first fetch that the grant it handed out is not
        // one. ⚠️ THE COST TABLE IS `free()` BECAUSE NO PRICE IS CONFIGURED
        // YET -- M9 owns the numbers, and a made-up price is worse than none.
        return new Capabilities(true, true, presigner != null, maxKeyBytes, MIN_PART_SIZE,
                CostTable.free());
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
            if (presigner != null) {
                presigner.close();
            }
        } finally {
            if (ownedCredentials != null) {
                try {
                    ownedCredentials.close();
                } catch (Exception failed) {
                    throw new IOException("closing the credential provider failed", failed);
                }
            }
        }
    }

    /**
     * Refuses a body that disagrees with itself BEFORE any request — answering
     * true where the write had already lost anyway.
     *
     * <p>⚠️ **THE CONTRACT RANKS TWO FAILURES AND THIS IS THE RANK.** The SPI
     * says a stale version yields EMPTY "even when the body is bad", and the
     * shared conformance suite has a case for exactly that combination —
     * M2.0's review found the in-memory backend ranking them the other way
     * round.
     *
     * <p>⚠️ **BEFORE THE REQUEST, WHICH IS WHAT MAKES THE ANSWER SAFE.** Two
     * earlier drafts ranked AFTER a failed attempt and both were wrong. The
     * first buffered the body in heap to do it, which is what
     * {@code Body.readFully}'s own javadoc forbids a streaming backend from
     * doing. The second ranked any transport failure, and review MEASURED the
     * hole with a client whose write LANDS and whose response is lost: the
     * object a {@code stat} then finds is OUR OWN, and the writer that won was
     * told it lost — the chain then carries the same batch twice. Validating
     * first removes the ambiguity rather than guessing at it: nothing has been
     * sent, so nothing can have landed.
     *
     * <p>⚠️ **IT READS THE BODY AND KEEPS NONE OF IT.** The stream is drained
     * to {@code nullOutputStream()}, so this is CPU and a second pass over the
     * caller's source, never heap — and everything this system writes
     * conditionally is a commit-log entry, a lease or an ordinal registry
     * (ADR-0002, ADR-0008).
     *
     * <p>⚠️ **IT OPENS THE BODY TWICE, AND THAT IS A PRECONDITION THIS PATH
     * ADDS.** {@code Body} promises each {@code open()} a fresh stream at
     * position zero; it does not promise the same BYTES twice, and this method
     * validates one stream while {@link #validatedBody} sends another. Every
     * conditional caller in the tree hands {@code Body.ofBytes} or a
     * deterministic encode, so nothing is broken today — but it is stated here
     * rather than assumed, because the failure would be a write that passed a
     * length check and sent something else. ⚠️ **AND THE SDK's OWN RETRIES DO
     * NOT ALREADY REQUIRE THIS on this path**: these two verbs carry
     * {@link #NO_RETRY} and get exactly one attempt, which this milestone
     * measures.
     *
     * <p>⚠️ **AND ONE {@code stat}, ON A DEFECT PATH ONLY**: it is issued when
     * a caller has handed a body that disagrees with itself, never on the
     * ordinary path, so cost rule R1 is untouched.
     *
     * @return true if the body was bad AND this write had already lost, in
     *     which case the caller answers empty
     * @throws IOException if the body was bad and the write had NOT already
     *     lost — a writer whose body is broken must never be told it lost a
     *     race nobody else was in, or its record is simply missing
     */
    private boolean refusedBeforeAnyRequest(String key, Body body, Version expected)
            throws IOException {
        Objects.requireNonNull(body, "body");
        try (InputStream in = body.checkedStream()) {
            in.transferTo(java.io.OutputStream.nullOutputStream());
            return false;
        } catch (IOException badBody) {
            Optional<ObjectStat> current;
            try {
                current = stat(key);
            } catch (IOException unreadable) {
                // ⚠️ THE ORIGINAL FAILURE IS WHAT THE CALLER NEEDS, and this
                // one is the reason it could not be ranked.
                unreadable.addSuppressed(badBody);
                throw unreadable;
            }
            boolean lost = expected == null
                    ? current.isPresent()
                    : current.isPresent() && !current.get().version().equals(expected);
            if (lost) {
                return true;
            }
            throw badBody;
        }
    }

    /**
     * The body of a conditional write, whose length {@link
     * #refusedBeforeAnyRequest} has already checked.
     *
     * <p>⚠️ **THE RAW STREAM, NOT {@code checkedStream}, AND ONLY BECAUSE THE
     * CHECK ALREADY RAN.** {@code checkedStream} verifies ON CLOSE, and the SDK
     * closes an abandoned body when a request fails for reasons of its own — so
     * a CONNECT FAILURE arrived at the caller as "body declared 1 bytes but
     * yielded 0", naming the wrong fault entirely. MEASURED against a closed
     * port. The unconditional {@link #put} keeps {@code checkedStream}, because
     * there the close-time verification is the only enforcement there is.
     */
    private static RequestBody validatedBody(Body body) {
        return RequestBody.fromContentProvider(body.open()::get, body.length(),
                "application/octet-stream");
    }

    private static RequestBody requestBody(Body body) {
        Objects.requireNonNull(body, "body");
        // ⚠️ `checkedStream`, NOT `open()` RAW: the SPI's own body contract is
        // that the declared length is the truth, and a stream that yields fewer
        // bytes than it promised would otherwise be signed and sent as a short
        // object that every reader afterwards parses as truncated.
        return RequestBody.fromContentProvider(body::checkedStream, body.length(),
                "application/octet-stream");
    }

    private static boolean isPreconditionFailed(S3Exception failed) {
        // ⚠️ THE STATUS, NOT THE MESSAGE. S3 says `PreconditionFailed`, MinIO
        // says `At least one of the pre-conditions you specified did not hold`,
        // and a third implementation will say something else -- the status code
        // is the part of the protocol all of them agree on.
        return failed.statusCode() == 412;
    }

    private static boolean isNotFound(S3Exception failed) {
        return failed.statusCode() == 404 || failed instanceof NoSuchKeyException;
    }

    @FunctionalInterface
    private interface Call<T> {
        T run();
    }

    private static <T> T translating(String what, Call<T> call) throws IOException {
        try {
            return call.run();
        } catch (java.io.UncheckedIOException unwrapped) {
            // ⚠️ THE SPI DECLARES `IOException` AND THIS IS WHERE THE SDK HIDES
            // ONE. A body refused by `checkedStream` is raised from inside the
            // SDK's own machinery, which wraps it -- so a caller catching what
            // the SPI promises was unwound by an unchecked throw. MEASURED:
            // `java.io.UncheckedIOException: java.io.IOException: body declared
            // 1 bytes but yielded 0`.
            throw new IOException(what + " failed: " + unwrapped.getCause().getMessage(),
                    unwrapped.getCause());
        } catch (SdkException failed) {
            throw asIoException(what, failed);
        }
    }

    /**
     * ⚠️ **THE KEY IS IN THE MESSAGE AND THE CREDENTIAL NEVER IS** (security.md
     * rule 4). Whatever this throws may be chained, printed and pasted into a
     * ticket; the SDK's own message names the request and the status, which is
     * what a reader needs, and the cause is kept so a retry decision upstream
     * can still inspect it.
     */
    private static IOException asIoException(String what, Exception failed) {
        return new IOException(what + " failed: " + failed.getMessage(), failed);
    }
}
