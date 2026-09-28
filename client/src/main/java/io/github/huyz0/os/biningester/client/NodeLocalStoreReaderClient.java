// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.OptionalLong;

/** Plugin-side bounded HTTP client; its only credential source is the installation secret file. */
public final class NodeLocalStoreReaderClient implements AutoCloseable {
    private final HttpClient client;
    private final URI endpoint;
    private final String authorization;
    private final long maxBytes;
    private final Duration bodyDeadline;

    /** Composition-root helper that keeps URI and filesystem types out of the plugin module. */
    public static NodeLocalStoreReaderClient open(String endpoint, String secretFile,
            Duration timeout, long maxBytes) throws IOException {
        return new NodeLocalStoreReaderClient(URI.create(endpoint), Path.of(secretFile), timeout,
                maxBytes);
    }

    /**
     * ⚠️ M10.22: THE REQUEST TIMEOUT BOUNDS THE HEADERS ONLY. A reader that
     * answers 200 and then stalls mid-body would hold its caller -- a Tier-2
     * read, and whatever lock it runs under -- for as long as the loopback
     * socket stays open. So the body has its own deadline, past which the
     * body is closed and its next read fails. ⚠️ COUNTED FROM THE HEADERS, not
     * the request (M11.12, H7; M10.22 review R2): the worst case is the request
     * timeout for the headers plus this for the body -- about 60 s at the
     * defaults.
     */
    static final Duration DEFAULT_BODY_DEADLINE = Duration.ofSeconds(30);

    /**
     * ⚠️ REMOVE-ON-CANCEL (M11.12, H7; M10.22 review R4): a body closed in
     * time cancels its timer, and a cancelled timer left queued for its full
     * delay is one more object per read held for 30 s.
     */
    private static final java.util.concurrent.ScheduledThreadPoolExecutor DEADLINES =
            deadlines();

    private static java.util.concurrent.ScheduledThreadPoolExecutor deadlines() {
        var executor = new java.util.concurrent.ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "node-local-reader-deadline");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /** Body deadlines scheduled and not yet run or cancelled. */
    static int pendingDeadlines() {
        return DEADLINES.getQueue().size();
    }

    public NodeLocalStoreReaderClient(URI endpoint, Path secretFile, Duration timeout,
            long maxBytes) throws IOException {
        this(endpoint, secretFile, timeout, maxBytes, DEFAULT_BODY_DEADLINE);
    }

    /** The same, with the body's own deadline. */
    public NodeLocalStoreReaderClient(URI endpoint, Path secretFile, Duration timeout,
            long maxBytes, Duration bodyDeadline) throws IOException {
        if (!"http".equals(endpoint.getScheme()) || !"127.0.0.1".equals(endpoint.getHost())
                || endpoint.getUserInfo() != null || endpoint.getRawQuery() != null
                || endpoint.getFragment() != null || endpoint.getPort() < 1
                || !(endpoint.getRawPath().isEmpty() || endpoint.getRawPath().equals("/"))) {
            throw new IllegalArgumentException("reader endpoint must use loopback HTTP");
        }
        if (maxBytes < 1 || maxBytes > 64L << 20) {
            throw new IllegalArgumentException("reader response limit is invalid");
        }
        byte[] secret = InstallationSecret.read(secretFile);
        this.endpoint = endpoint;
        this.authorization = "Bearer " + Base64.getUrlEncoder().withoutPadding().encodeToString(secret);
        java.util.Arrays.fill(secret, (byte) 0);
        this.maxBytes = maxBytes;
        this.bodyDeadline = java.util.Objects.requireNonNull(bodyDeadline, "bodyDeadline");
        if (bodyDeadline.isNegative() || bodyDeadline.isZero()) {
            throw new IllegalArgumentException("a body deadline is positive");
        }
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public InputStream get(String bucket, String prefix, String key) throws IOException {
        return getIfPresent(bucket, prefix, key).orElseThrow(
                () -> new IOException("node-local reader answered HTTP 404"));
    }

    /** One bounded GET; a missing next-chain slot is an ordinary empty result. */
    public Optional<InputStream> getIfPresent(String bucket, String prefix, String key)
            throws IOException {
        HttpRequest request = request("/v1/object", bucket, prefix, key);
        try {
            HttpResponse<InputStream> response = client.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() == 404) {
                return missingResponse(response.body());
            }
            if (response.statusCode() != 200) {
                response.body().close();
                throw new IOException("node-local reader answered HTTP " + response.statusCode());
            }
            return Optional.of(new LimitedInputStream(deadlined(response.body()), maxBytes));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("node-local reader request interrupted");
        }
    }

    /** {@code body}, closed underneath its reader once {@code bodyDeadline} has passed. */
    private InputStream deadlined(InputStream body) {
        java.util.concurrent.atomic.AtomicBoolean expired =
                new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.ScheduledFuture<?> timer = DEADLINES.schedule(() -> {
            expired.set(true);
            try {
                body.close();
            } catch (IOException ignored) {
                // closing is the whole action; a failure to close leaves the read to fail
            }
        }, bodyDeadline.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
        return new java.io.FilterInputStream(body) {
            @Override public int read() throws IOException {
                try {
                    return check(super.read());
                } catch (IOException failed) {
                    throw expiredOr(failed);
                }
            }

            @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                try {
                    return check(super.read(bytes, offset, length));
                } catch (IOException failed) {
                    throw expiredOr(failed);
                }
            }

            private int check(int result) throws IOException {
                if (expired.get()) {
                    throw deadline(null);
                }
                return result;
            }

            /**
             * ⚠️ THE DEADLINE BY NAME (M11.12, H7; M10.22 review R3): a read
             * blocked when the timer closes the body fails with the JDK's
             * "closed", which says nothing of why.
             */
            private IOException expiredOr(IOException failed) {
                String message = failed.getMessage();
                return expired.get() && (message == null || !message.contains(" deadline"))
                        ? deadline(failed) : failed;
            }

            private IOException deadline(IOException cause) {
                return new IOException("node-local reader body exceeded its " + bodyDeadline
                        + " deadline", cause);
            }

            @Override public void close() throws IOException {
                timer.cancel(false);
                super.close();
            }
        };
    }

    static Optional<InputStream> missingResponse(InputStream body) throws IOException {
        body.close();
        return Optional.empty();
    }

    public OptionalLong stat(String bucket, String prefix, String key) throws IOException {
        HttpRequest request = request("/v1/stat", bucket, prefix, key);
        try {
            HttpResponse<InputStream> response = client.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() == 404) return OptionalLong.empty();
            if (response.statusCode() != 200) {
                response.body().close();
                throw new IOException("node-local reader answered HTTP " + response.statusCode());
            }
            try (InputStream body = response.body()) {
                byte[] bytes = body.readNBytes(21);
                if (bytes.length == 21 || bytes.length == 0) {
                    throw new IOException("node-local reader returned an invalid object size");
                }
                return OptionalLong.of(Long.parseLong(new String(bytes,
                        java.nio.charset.StandardCharsets.US_ASCII)));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("node-local reader request interrupted");
        } catch (NumberFormatException malformed) {
            throw new IOException("node-local reader returned an invalid object size");
        }
    }

    private HttpRequest request(String path, String bucket, String prefix, String key) {
        return HttpRequest.newBuilder().uri(endpoint.resolve(path)).timeout(Duration.ofSeconds(30))
                .header("Authorization", authorization).header("X-Bin-Bucket", bucket)
                .header("X-Bin-Prefix", prefix).header("X-Bin-Key", key).GET().build();
    }

    @Override public void close() { client.close(); }

    private static final class LimitedInputStream extends java.io.FilterInputStream {
        private long remaining;
        LimitedInputStream(InputStream input, long limit) { super(input); remaining = limit; }
        @Override public int read() throws IOException {
            if (remaining == 0) {
                if (in.read() < 0) return -1;
                throw new IOException("node-local reader response exceeds configured limit");
            }
            int value = in.read();
            if (value >= 0) remaining--;
            return value;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) return 0;
            if (remaining == 0) {
                if (in.read() < 0) return -1;
                throw new IOException("node-local reader response exceeds configured limit");
            }
            int read = in.read(bytes, offset, (int) Math.min(length, remaining));
            if (read > 0) remaining -= read;
            return read;
        }
    }
}
