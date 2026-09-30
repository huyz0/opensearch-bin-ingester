// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * M12.7 (M11.6 P1): every {@code 429} leaves through
 * {@code BulkService.tooManyRequests}, the one place that sets
 * {@code Retry-After} -- a second emitter is how a bare {@code 429}, retried at
 * whatever rate the producer's loop runs, comes back.
 */
class SingleTooManyRequestsGateTest {

    private static final String OWNER =
            "http/src/main/java/io/github/huyz0/os/biningester/http/BulkService.java";
    private static final String ONE = "response.status(Status.TOO_MANY_REQUESTS_429)";

    @Test
    void onlyTheOwnerMaySendA429AndOnlyOnce() throws Exception {
        Path root = scratch("owner");
        try {
            Path owner = write(root, OWNER, "void tooManyRequests() { " + ONE + "; }\n"
                    + "// a comment naming TOO_MANY_REQUESTS_429 is not a second one");
            Path other = write(root,
                    "server/src/main/java/io/github/huyz0/os/biningester/server/Door.java",
                    "res.status(Status.create(429)).send();");
            Path literal = write(root,
                    "http/src/main/java/io/github/huyz0/os/biningester/http/Other.java",
                    "res.status( 429 ).send();");
            Path test = write(root,
                    "http/src/test/java/io/github/huyz0/os/biningester/http/FakeTest.java",
                    ONE + ";");
            Path string = write(root,
                    "http/src/main/java/io/github/huyz0/os/biningester/http/Says.java",
                    "String why = \"we answer TOO_MANY_REQUESTS_429 elsewhere\";");
            // ⚠️ THE JDK SERVER's SHAPE (M12.7 review P1): a status set as a plain
            // int, as NodeLocalStoreReaderMain sets its 403, 404, 405 and 500.
            Path jdk = write(root,
                    "server/src/main/java/io/github/huyz0/os/biningester/server/Reader.java",
                    "respond(exchange, 429, new byte[0]);");
            Path notJava = write(root,
                    "server/src/main/resources/limits.properties", "status=429");
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.singleTooManyRequests(root,
                    List.of(owner, other, literal, test, string, jdk, notJava), failures);

            assertThat(failures).hasSize(3)
                    .anyMatch(message -> message.contains("server/Door.java"))
                    .anyMatch(message -> message.contains("http/Other.java"))
                    .anyMatch(message -> message.contains("server/Reader.java"));
        } finally {
            delete(root);
        }
    }

    @Test
    void aSecond429InTheOwnerIsRefusedToo() throws Exception {
        Path root = scratch("twice");
        try {
            Path owner = write(root, OWNER, "void tooManyRequests() { " + ONE + "; }\n"
                    + "void shortcut() { " + ONE + ".send(); }");
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.singleTooManyRequests(root, List.of(owner), failures);

            assertThat(failures).singleElement().satisfies(message ->
                    assertThat(message).contains(OWNER).contains("in 2 places"));
        } finally {
            delete(root);
        }
    }

    /** ⚠️ THE METHOD, NOT MERELY THE FILE (M12.7 review P2): Retry-After is set there. */
    @Test
    void theOwnersOne429MustBeInTooManyRequests() throws Exception {
        Path root = scratch("elsewhere");
        try {
            Path owner = write(root, OWNER, "static void tooManyRequests(ServerResponse r) {\n"
                    + "    r.header(\"Retry-After\", \"1\");\n}\n"
                    + "void shortcut(ServerResponse r) { r." + ONE.substring(ONE.indexOf('.') + 1)
                    + ".send(); }");
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.singleTooManyRequests(root, List.of(owner), failures);

            assertThat(failures).singleElement().satisfies(message ->
                    assertThat(message).contains(OWNER).contains("outside tooManyRequests"));
        } finally {
            delete(root);
        }
    }

    @Test
    void theTreeSendsItsOne429FromTheOwnerAndTheGatesRunIt() throws Exception {
        Path root = repository();
        List<Path> main = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(path -> path.toString().replace('\\', '/').contains("/src/main/java/"))
                    .filter(path -> !path.toString().replace('\\', '/').contains("/build/"))
                    .filter(path -> path.toString().endsWith(".java"))
                    .forEach(main::add);
        }
        List<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.singleTooManyRequests(root, main, failures);

        assertThat(RepositoryGateChecks.TOO_MANY_REQUESTS_OWNER).isEqualTo(OWNER);
        assertThat(failures).as("this tree").isEmpty();
        // ⚠️ AND THE GATES RUN IT, BY BEHAVIOUR (M13.15, M11.24 review T4): the
        // check listed under this name refuses a second 429 sender.
        Path scratch = scratch("wired");
        try {
            Path other = write(scratch,
                    "server/src/main/java/io/github/huyz0/os/biningester/server/Door.java",
                    "res.status(Status.create(429)).send();");
            List<String> wired = new ArrayList<>();
            RepositoryChecks.INSTANCE.named("singleTooManyRequests").getRun()
                    .invoke(scratch, List.of(other), wired);
            assertThat(wired).anyMatch(message -> message.contains("server/Door.java"));
        } finally {
            delete(scratch);
        }
    }

    private static Path write(Path root, String relative, String body) throws Exception {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, body + "\n");
        return file;
    }

    /** The checkout's root: the harness runs with {@code buildSrc} as its working directory. */
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }

    /** Under {@code buildSrc/build/tmp}, never the system temp directory (testing.md rule 17). */
    private static Path scratch(String name) throws Exception {
        Path dir = repository().resolve("buildSrc/build/tmp/single-429")
                .resolve(name + "-" + UUID.randomUUID());
        Files.createDirectories(dir);
        return dir;
    }

    private static void delete(Path root) throws Exception {
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
