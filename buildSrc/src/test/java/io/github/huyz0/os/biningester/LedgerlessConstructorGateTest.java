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
 * M12.1 (M11 review F2): no production source outside the composition root
 * makes an {@code IndexCostLedger}. A class that makes its own charges a
 * ledger nothing reads, so a read path built with one issues requests no
 * report can see, and "shares sum to counted requests" breaks silently.
 */
class LedgerlessConstructorGateTest {

    private static final String OWNER =
            "server/src/main/java/io/github/huyz0/os/biningester/server/StoreStack.java";

    @Test
    void onlyTheOwnerMayMakeALedgerInProductionSource() throws Exception {
        Path root = scratch("owner");
        try {
            Path owner = write(root, OWNER, "IndexCostLedger l = new IndexCostLedger();");
            Path violator = write(root,
                    "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/Proxy.java",
                    "this(store, new IndexCostLedger ());");
            Path test = write(root,
                    "ingest/src/test/java/io/github/huyz0/os/biningester/ingest/ProxyTest.java",
                    "new Proxy(store, new IndexCostLedger());");
            Path comment = write(root,
                    "http/src/main/java/io/github/huyz0/os/biningester/http/Commented.java",
                    "// an earlier draft called new IndexCostLedger() here\nclass Commented {}");
            // ⚠️ THE OTHER SPELLINGS (M12.1 review P1, T1): package-qualified, as
            // five test call sites write it, and a constructor reference.
            Path qualified = write(root,
                    "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/Publisher.java",
                    "this(store, new io.github.huyz0.os.biningester.binstore.IndexCostLedger());");
            Path reference = write(root,
                    "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/Responder.java",
                    "Supplier<IndexCostLedger> ledgers = IndexCostLedger::new;");
            List<String> failures = new ArrayList<>();

            RepositoryGateChecks.INSTANCE.ledgerOwnership(root,
                    List.of(owner, violator, test, comment, qualified, reference), failures);

            assertThat(failures).hasSize(3)
                    .anyMatch(message -> message.contains("ingest/Proxy.java"))
                    .anyMatch(message -> message.contains("ingest/Publisher.java"))
                    .anyMatch(message -> message.contains("ingest/Responder.java"))
                    .allMatch(message -> message.contains("IndexCostLedger"));
        } finally {
            delete(root);
        }
    }

    @Test
    void theTreeMakesItsOneLedgerInTheOwnerOnly() throws Exception {
        Path root = repository();
        List<Path> main = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(path -> path.toString().replace('\\', '/').contains("/src/main/java/"))
                    .filter(path -> !path.toString().replace('\\', '/').contains("/build/"))
                    .filter(path -> path.toString().endsWith(".java"))
                    .forEach(main::add);
        }
        List<String> failures = new ArrayList<>();

        RepositoryGateChecks.INSTANCE.ledgerOwnership(root, main, failures);

        assertThat(RepositoryGateChecks.LEDGER_OWNER).isEqualTo(OWNER);
        assertThat(failures).as("this tree, after M12.1").isEmpty();
        assertThat(Files.readString(root.resolve(OWNER))).contains("new IndexCostLedger()");
    }

    /** ⚠️ BY BEHAVIOUR, NOT TEXT (M13.15, M11.24 review T4): see RepositoryChecks. */
    @Test
    void theRepositoryGatesTaskRunsTheOwnershipCheck() throws Exception {
        Path root = scratch("wired");
        try {
            Path violator = write(root,
                    "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/Proxy.java",
                    "this(store, new IndexCostLedger());");
            List<String> failures = new ArrayList<>();

            RepositoryChecks.INSTANCE.named("ledgerOwnership").getRun()
                    .invoke(root, List.of(violator), failures);

            assertThat(failures).singleElement().asString().contains("ingest/Proxy.java");
        } finally {
            delete(root);
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
        Path dir = repository().resolve("buildSrc/build/tmp/ledger-ownership")
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
