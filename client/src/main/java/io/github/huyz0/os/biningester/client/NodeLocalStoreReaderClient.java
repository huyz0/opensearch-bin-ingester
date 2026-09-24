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

    /** Composition-root helper that keeps URI and filesystem types out of the plugin module. */
    public static NodeLocalStoreReaderClient open(String endpoint, String secretFile,
            Duration timeout, long maxBytes) throws IOException {
        return new NodeLocalStoreReaderClient(URI.create(endpoint), Path.of(secretFile), timeout,
                maxBytes);
    }

    public NodeLocalStoreReaderClient(URI endpoint, Path secretFile, Duration timeout,
            long maxBytes) throws IOException {
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
            return Optional.of(new LimitedInputStream(response.body(), maxBytes));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("node-local reader request interrupted");
        }
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
