// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.client.InstallationSecret;
import io.github.huyz0.os.biningester.sequencer.NodeLocalStoreReaderKeyPolicy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Standalone node-local process. It owns the store SDK and accepts only loopback HTTP. */
public final class NodeLocalStoreReaderMain {
    private NodeLocalStoreReaderMain() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--init-secret")) {
            InstallationSecret.create(Path.of(args[1]));
            return;
        }
        if (args.length != 1) {
            System.err.println("usage: node-local-store-reader <config.properties> | --init-secret <path>");
            System.exit(2);
            return;
        }
        Properties config = new Properties();
        try (var input = java.nio.file.Files.newInputStream(Path.of(args[0]))) {
            config.load(input);
        }
        RunningReader reader = start(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                reader.close();
            } catch (IOException failed) {
                System.err.println("node-local reader shutdown failed: " + failed.getClass().getSimpleName());
            } finally {
                stopped.countDown();
            }
        }, "node-local-reader-shutdown"));
        try {
            System.err.println("node-local reader listening on 127.0.0.1:" + reader.port());
            stopped.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Starts the same bounded service used by the executable, for embedding and component tests. */
    public static RunningReader start(Properties config) throws IOException {
        if (!"127.0.0.1".equals(config.getProperty("reader.bind"))) {
            throw new IllegalArgumentException("reader.bind must be 127.0.0.1");
        }
        int port = validatePort(Integer.parseInt(config.getProperty("reader.port", "0")));
        String bucket = required(config, "reader.bucket");
        String prefix = required(config, "store.prefix");
        StoreConfig storeConfig = new StoreConfig(required(config, "store.kind"),
                Optional.ofNullable(config.getProperty("store.root")),
                Optional.ofNullable(config.getProperty("store.endpoint")),
                Optional.ofNullable(config.getProperty("store.region")),
                Optional.ofNullable(config.getProperty("store.bucket")),
                Boolean.parseBoolean(config.getProperty("store.path-style", "false")));
        validateReaderBucket(storeConfig, bucket);
        long maxBytes = Long.parseLong(config.getProperty("reader.max-object-bytes",
                Long.toString(NodeLocalStoreReader.DEFAULT_MAX_OBJECT_BYTES)));
        byte[] secret = InstallationSecret.read(Path.of(required(config, "reader.secret-file")));
        BinStore store = StoreFactory.open(storeConfig);
        try {
            NodeLocalStoreReader reader = new NodeLocalStoreReader(store,
                    new NodeLocalStoreReaderKeyPolicy(bucket, prefix), secret, maxBytes);
            java.util.Arrays.fill(secret, (byte) 0);
            HttpServer server = HttpServer.create(new InetSocketAddress(
                    InetAddress.getByName("127.0.0.1"), port), 0);
            server.createContext("/v1/object", exchange -> handleGet(exchange, reader));
            server.createContext("/v1/stat", exchange -> handleStat(exchange, reader));
            server.start();
            return new RunningReader(server, store);
        } catch (IOException | RuntimeException failed) {
            try {
                store.close();
            } catch (IOException closeFailed) {
                failed.addSuppressed(closeFailed);
            }
            throw failed;
        } finally {
            java.util.Arrays.fill(secret, (byte) 0);
        }
    }

    static int validatePort(int port) {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("reader.port is invalid");
        return port;
    }

    static void validateReaderBucket(StoreConfig storeConfig, String readerBucket) {
        if ("s3".equals(storeConfig.normalizedKind())) {
            String storeBucket = storeConfig.bucket().orElseThrow(() ->
                    new IllegalArgumentException("store.bucket is required for the node-local reader"));
            if (!storeBucket.equals(readerBucket)) {
                throw new IllegalArgumentException("reader.bucket must match store.bucket");
            }
        }
    }

    public static final class RunningReader implements AutoCloseable {
        private final HttpServer server;
        private final BinStore store;

        private RunningReader(HttpServer server, BinStore store) {
            this.server = server;
            this.store = store;
        }

        public int port() { return server.getAddress().getPort(); }

        @Override public void close() throws IOException {
            server.stop(1);
            store.close();
        }
    }

    private static void handleGet(HttpExchange exchange, NodeLocalStoreReader reader)
            throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) { respond(exchange, 405, new byte[0]); return; }
        try (var object = reader.get(exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestHeaders().getFirst("X-Bin-Bucket"),
                exchange.getRequestHeaders().getFirst("X-Bin-Prefix"),
                exchange.getRequestHeaders().getFirst("X-Bin-Key"))) {
            exchange.sendResponseHeaders(200, 0);
            try (var out = exchange.getResponseBody()) { object.transferTo(out); }
        } catch (SecurityException | IllegalArgumentException refused) {
            respond(exchange, 403, new byte[0]);
        } catch (IOException failed) {
            if (exchange.getResponseCode() < 0) respond(exchange, 500, new byte[0]);
            exchange.close();
        }
    }

    private static void handleStat(HttpExchange exchange, NodeLocalStoreReader reader)
            throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) { respond(exchange, 405, new byte[0]); return; }
        try {
            Optional<ObjectStat> stat = reader.stat(exchange.getRequestHeaders().getFirst("Authorization"),
                    exchange.getRequestHeaders().getFirst("X-Bin-Bucket"),
                    exchange.getRequestHeaders().getFirst("X-Bin-Prefix"),
                    exchange.getRequestHeaders().getFirst("X-Bin-Key"));
            respond(exchange, stat.isPresent() ? 200 : 404,
                    stat.map(value -> Long.toString(value.size()).getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                            .orElseGet(() -> new byte[0]));
        } catch (SecurityException | IllegalArgumentException refused) {
            respond(exchange, 403, new byte[0]);
        } catch (IOException failed) {
            respond(exchange, 500, new byte[0]);
        }
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value;
    }
}
