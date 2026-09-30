// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The single-node RustFS container every T3 case in this module shares (M8.2,
 * testing.md rule 19a).
 *
 * <p>⚠️ **RUSTFS IS A TEST FIXTURE, NOT A SUPPORTED BACKEND.** It is a local
 * stand-in for S3's wire protocol so a test need not reach AWS. The conformance
 * cases assert the shapes this project depends on, including a 412 for a lost
 * conditional write and a 404 for a conditional write against an absent key.
 *
 * <p>⚠️ **SINGLE-NODE ONLY.** The fixture qualifies the endpoint used by the
 * tests. It does not qualify conditional-write atomicity across multiple RustFS
 * endpoints.
 *
 * <p>⚠️ **`docker compose`, NOT Testcontainers.** The compose file pins the
 * image by digest and declares the memory cap `./gradlew gates` reads
 * ({@code TestBudget}); Testcontainers would be a second description of the same container
 * plus a dependency tree this build would have to pin a sha and a licence for,
 * jar by jar.
 *
 * <p>⚠️ **PORT 0.** The compose file asks the kernel for a port and this reads
 * it back, so two sessions cannot collide.
 *
 * <p>⚠️ **STARTED ONCE PER JVM AND LEFT RUNNING.** Composing up and down per
 * class costs seconds each time and buys nothing: every case makes its own
 * bucket.
 */
public final class S3Fixture {

    public static final String ACCESS_KEY = "rustfsadmin";
    public static final String SECRET_KEY = "rustfsadmin";

    private static final Object LOCK = new Object();
    private static String endpoint;

    private S3Fixture() {
    }

    /** Whether a Docker daemon is reachable at all. */
    public static boolean dockerAvailable() {
        try {
            return run(List.of("docker", "info"), 20).exitCode() == 0;
        } catch (RuntimeException unreachable) {
            return false;
        }
    }

    /** The endpoint of a healthy RustFS fixture, starting it on first use. */
    public static String endpoint() {
        synchronized (LOCK) {
            if (endpoint == null) {
                endpoint = start();
            }
            return endpoint;
        }
    }

    private static String start() {
        Path compose = Path.of(System.getProperty("io.github.huyz0.os.biningester.repoRoot", "."))
                .resolve("docker-compose.test.yml");
        Result up = run(List.of("docker", "compose", "-f", compose.toString(),
                "up", "-d", "--wait", "rustfs"), 180);
        if (up.exitCode() != 0) {
            throw new IllegalStateException("RustFS did not start: " + up.output());
        }
        Result port = run(List.of("docker", "compose", "-f", compose.toString(),
                "port", "rustfs", "9000"), 30);
        if (port.exitCode() != 0 || port.output().isBlank()) {
            throw new IllegalStateException("RustFS published no port: " + port.output());
        }
        // ⚠️ THE HOST HALF IS DISCARDED. Compose answers `0.0.0.0:32769`, and a
        // client that dialled 0.0.0.0 would be dialling every interface.
        String published = port.output().trim().lines().findFirst().orElseThrow();
        return "http://localhost:" + published.substring(published.lastIndexOf(':') + 1);
    }

    private record Result(int exitCode, String output) {
    }

    private static Result run(List<String> command, int timeoutSeconds) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            // ⚠️ DRAIN ON ANOTHER THREAD so a hung Docker command still reaches
            // the timeout and reports the command rather than hanging the suite.
            var output = new java.io.ByteArrayOutputStream();
            Thread drain = Thread.ofVirtual().start(() -> {
                try (var in = process.getInputStream()) {
                    in.transferTo(output);
                } catch (IOException closed) {
                    // The process died; whatever was read is what is reported.
                }
            });
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                drain.join();
                throw new IllegalStateException(command + " did not finish in "
                        + timeoutSeconds + "s");
            }
            drain.join();
            return new Result(process.exitValue(), output.toString(StandardCharsets.UTF_8));
        } catch (IOException failed) {
            throw new UncheckedIOException(failed);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(command + " was interrupted", interrupted);
        }
    }
}
