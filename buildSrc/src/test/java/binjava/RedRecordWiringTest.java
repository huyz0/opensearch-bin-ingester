// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ⚠️ THAT THE GATE ACTUALLY USES THE NEW BINDING (M0.56).
 *
 * <p>{@code RedRecordBindingTest} exercises {@code binding_key} through the
 * {@code key} CLI seam, which proves the function is right and proves nothing
 * about whether {@code check()} calls it. Review measured the gap: reverting
 * the two shipped call sites to {@code sha256(whole file)} left every test in
 * this module green while the churn returned verbatim and {@code binding_key}
 * became a correct, fully-tested, dead function.
 *
 * <p>⚠️ That is the exact shape {@code TddPlanScopeTest} was written for one
 * file over — a CLI seam tested, a shipped path not — so this repo has already
 * paid for the lesson once. This test drives the gate itself, over a synthetic
 * repository, and fails if the wiring is reverted.
 */
class RedRecordWiringTest {

  @TempDir Path tmp;

  @Test
  void checkUsesTheNarrowBindingSoAnAddedTestDoesNotStaleTheOthers() throws Exception {
    Path src = Path.of("m/src/test/java/p/AlphaTest.java");
    // ⚠️ The order is the whole test. The record for `alpha` is written while
    // the file holds only `alpha`; `beta` is appended afterwards. Under the
    // narrow binding `alpha`'s key is unchanged and the gate passes. Under the
    // whole-file binding it replaced, appending anything makes `alpha`'s stored
    // hash stale and the gate fails -- which is what a reverted wiring does.
    write(src, testFile("""
            @Test
            void alpha() {
                assertThat(helper()).isEqualTo(1);
            }
        """));
    git("add", "-A");
    record("p.AlphaTest#alpha", src);

    write(src, testFile("""
            @Test
            void alpha() {
                assertThat(helper()).isEqualTo(1);
            }

            @Test
            void beta() {
                assertThat(helper()).isEqualTo(1);
            }
        """));
    git("add", "-A");
    record("p.AlphaTest#beta", src);

    Run r = run("python3", "scripts/tdd_scan.py", "check");
    assertThat(r.out)
        .as("appending a test must not stale the record of the one beside it")
        .doesNotContain("changed after their red run");
    assertThat(r.out)
        .as("and it actually EXAMINED them -- absence of a failure is also what a "
            + "gate that skipped every id would print")
        .contains("2 new test(s)");
    assertThat(r.exit).as(r.out).isZero();
  }

  @Test
  void checkStillCatchesTheRecordedTestBeingEdited() throws Exception {
    // ⚠️ The half that must survive the narrowing. If only the first test
    // existed, deleting the staleness comparison outright would pass.
    Path src = Path.of("m/src/test/java/p/AlphaTest.java");
    write(src, testFile("""
            @Test
            void alpha() {
                assertThat(helper()).isEqualTo(1);
            }
        """));
    git("add", "-A");
    record("p.AlphaTest#alpha", src);

    write(src, testFile("""
            @Test
            void alpha() {
                assertThat(helper()).isEqualTo(999);
            }
        """));
    git("add", "-A");

    Run r = run("python3", "scripts/tdd_scan.py", "check");
    assertThat(r.out).contains("changed after their red run");
    assertThat(r.exit).as(r.out).isNotZero();
  }

  // ---- fixture plumbing -------------------------------------------------

  private static String testFile(String methods) {
    return "package p;\n\nimport org.junit.jupiter.api.Test;\n\n"
        + "class AlphaTest {\n\n"
        + "    private static int helper() {\n        return 1;\n    }\n\n"
        + methods
        + "}\n";
  }

  private void write(Path rel, String body) throws IOException {
    Path f = tmp.resolve(rel);
    Files.createDirectories(f.getParent());
    Files.writeString(f, body);
  }

  /** Writes a red record carrying the key the CURRENT binding computes. */
  private void record(String ident, Path src) throws Exception {
    Run k = run("python3", "scripts/tdd_scan.py", "key", src.toString(), ident);
    assertThat(k.exit).as("key %s: %s", ident, k.out).isZero();
    Path rec = tmp.resolve(".harness/tdd/red.json");
    Files.createDirectories(rec.getParent());
    String existing = Files.exists(rec) ? Files.readString(rec) : "{\"red\":{}}";
    String entry =
        "\"%s\": {\"at\": 0, \"source\": \"%s\", \"sha256\": \"%s\"}"
            .formatted(ident, src, k.out.trim());
    Files.writeString(
        rec,
        existing.contains("\"red\": {}") || existing.equals("{\"red\":{}}")
            ? "{\"red\": {%s}}".formatted(entry)
            : existing.replaceFirst("\\{\"red\": \\{", "{\"red\": {" + entry + ", "));
  }

  private record Run(int exit, String out) {}

  private Run run(String... cmd) throws IOException, InterruptedException {
    Process p =
        new ProcessBuilder(cmd).directory(tmp.toFile()).redirectErrorStream(true).start();
    return new Run(p.waitFor(), new String(p.getInputStream().readAllBytes()));
  }

  private void git(String... args) throws IOException, InterruptedException {
    Run r = run(concat(new String[] {"git"}, args));
    assertThat(r.exit).as("git %s: %s", String.join(" ", args), r.out).isZero();
  }

  private static String[] concat(String[] a, String[] b) {
    String[] out = new String[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  @org.junit.jupiter.api.BeforeEach
  void repo() throws Exception {
    git("init", "-q");
    git("config", "user.email", "t@t");
    git("config", "user.name", "t");
    git("commit", "-q", "--allow-empty", "-m", "base");
    Path scripts = tmp.resolve("scripts");
    Files.createDirectories(scripts);
    Path real = Path.of("..").toAbsolutePath().normalize().resolve("scripts");
    // ⚠️ `git_renames.py` travels with the scanner for the same reason
    // `java_tests.py` does: `tdd_scan` imports it (M0.53), so a fixture
    // without it fails on an ImportError rather than on its subject.
    for (String f : List.of("tdd_scan.py", "java_tests.py", "git_renames.py")) {
      Files.copy(real.resolve(f), scripts.resolve(f));
    }
  }
}
