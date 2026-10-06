// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.gradle.api.GradleException;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;

/**
 * M13.15 (M12 harvest R13): the build's wiring, pinned by what runs rather
 * than by the text of one call (M11.24 review T4; M11.25 review T2).
 *
 * <p>{@code verify()} is RUN, on a scratch git repository through
 * {@link ProjectBuilder} (review T1), and the list is checked against the
 * predicates it must hold, derived by reflection (review T2).
 *
 * <p>⚠️ WHAT IS STILL READ AS TEXT, AND WHY: the build scripts configuring every
 * {@code Test} task. Evaluating the module scripts means building the real
 * multi-project build inside a test; they are read as CODE instead -- comments
 * stripped, by {@link LegacyScriptQuarantine#code} -- with the property
 * required at a {@code withType<Test>} block's top level and nowhere else.
 */
class RepositoryChecksTest {

    private static final String PROPERTY =
            "systemProperty(\"junit.jupiter.execution.timeout.threaddump.enabled\", \"true\")";

    /**
     * ⚠️ EVERY PREDICATE, DERIVED (M13.15 review T2): the list's names are
     * exactly the {@code RepositoryGateChecks} predicates -- its public methods
     * taking the root, perhaps the files, and the failures -- so one added
     * there and left off the list, which the gates would never run, fails here.
     */
    @Test
    void everyListedCheckHasItsOwnName() {
        List<String> names = RepositoryChecks.INSTANCE.getALL().stream()
                .map(RepositoryCheck::getName).toList();
        List<String> predicates = java.util.Arrays.stream(
                        RepositoryGateChecks.class.getDeclaredMethods())
                .filter(m -> java.lang.reflect.Modifier.isPublic(m.getModifiers()))
                .filter(m -> m.getReturnType() == void.class)
                .filter(m -> List.of(List.of(Path.class, List.class),
                        List.of(Path.class, List.class, List.class))
                        .contains(List.of(m.getParameterTypes())))
                .map(java.lang.reflect.Method::getName).sorted().toList();

        assertThat(names).doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(predicates);
        assertThat(predicates).as("the premise: the predicates are found").hasSizeGreaterThan(10);
    }

    /**
     * ⚠️ THE LIST RUNS, BEFORE THE THROW: {@code verify()}'s one statement over
     * {@link RepositoryChecks#getALL}, as code, and ahead of the failure check.
     */
    @Test
    void verifyRunsEveryListedCheckBeforeItThrows() throws Exception {
        String task = code("buildSrc/src/main/kotlin/io/github/huyz0/os/biningester/"
                + "RepositoryGatesTask.kt");
        String verify = task.substring(task.indexOf("fun verify()"),
                task.indexOf("private fun trackedTree"));

        int runs = verify.indexOf("RepositoryChecks.ALL.forEach { it.run(root, files, failures) }");
        assertThat(runs).as("verify() runs the list").isNotNegative();
        assertThat(runs).as("before it throws").isLessThan(
                verify.indexOf("if (failures.isNotEmpty())"));
        assertThat(verify).as("⚠️ AND NO PREDICATE OBJECT IS CALLED OUTSIDE IT")
                .doesNotContain("RepositoryGateChecks.");
    }

    /**
     * ⚠️ {@code verify()} ITSELF, RUN (M13.15 review T1): the task, built by
     * {@link ProjectBuilder} over a scratch git repository holding one class
     * that makes its own ledger, refuses it by name. A guard around the list's
     * statement -- which the text check above lets through -- fails this.
     */
    @Test
    void theGatesTaskRefusesAListedChecksViolationWhenRun() throws Exception {
        Path repo = repository().resolve("buildSrc/build/tmp/repository-checks")
                .resolve("verify-" + java.util.UUID.randomUUID());
        try {
            // what verify() reads outright, empty: its own checks may refuse them
            for (String read : List.of(".pre-commit-config.yaml", "AGENTS.md")) {
                write(repo, read, "");
            }
            write(repo, "settings.gradle.kts", "rootProject.name = \"scratch\"\n");
            // ⚠️ WITH ITS HEADER (M13.15 review round 2 T1): headerless, the
            // task's own header check named the file too, and on Linux that
            // alone satisfied a message check -- with the list not run.
            write(repo, "ingest/src/main/java/io/github/huyz0/os/biningester/ingest/Proxy.java",
                    "// SPDX-License-Identifier: Apache-2.0\n"
                            + "class Proxy { Proxy() { this(new IndexCostLedger()); } }\n");
            git(repo, "init", "-q");
            git(repo, "add", "-A");
            RepositoryGatesTask task = ProjectBuilder.builder().withProjectDir(repo.toFile())
                    .build().getTasks().create("gates", RepositoryGatesTask.class);
            task.getRepository().set(repo.toFile());

            assertThatThrownBy(task::verify).isInstanceOf(GradleException.class)
                    .hasMessageContaining("ingest/Proxy.java makes an IndexCostLedger");
        } finally {
            delete(repo);
        }
    }

    private static void delete(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                path.toFile().setWritable(true); // git's object files are read-only on Windows
                Files.delete(path);
            }
        }
    }

    /**
     * ⚠️ EVERY TEST TASK PRINTS A THREAD DUMP ON TIMEOUT (M11.25 review T2): set
     * by construction on {@code withType<Test>} in the conventions plugin every
     * module applies, and in buildSrc's own build -- never per task, which a
     * task registered later did not inherit.
     */
    @Test
    void everyTestTaskGetsTheThreadDumpPropertyByConstruction() throws Exception {
        String conventions = code("buildSrc/src/main/kotlin/"
                + "io.github.huyz0.os.biningester.java-conventions.gradle.kts");
        // ⚠️ AT THE BLOCK's TOP LEVEL (M13.15 review T3): inside a nested
        // `if (name == "test") { .. }` it would reach one task, not every one.
        assertThat(withTypeTestBlocks(conventions)).as("the conventions plugin")
                .anyMatch(block -> topLevel(block).contains(PROPERTY));
        assertThat(withTypeTestBlocks(code("buildSrc/build.gradle.kts"))).as("buildSrc's build")
                .anyMatch(block -> topLevel(block).contains(PROPERTY));

        String settings = code("settings.gradle.kts");
        Matcher module = Pattern.compile("\"([a-z-]+)\"").matcher(
                settings.substring(settings.indexOf("include(")));
        // ⚠️ THE ROOT's TOO (review round 2 T3): a `subprojects { .. }` there
        // could set it again for every module.
        List<String> scripts = new java.util.ArrayList<>(List.of(conventions,
                code("buildSrc/build.gradle.kts"), code("build.gradle.kts")));
        while (module.find()) {
            String script = code(module.group(1) + "/build.gradle.kts");
            scripts.add(script);
            assertThat(script).as(module.group(1))
                    .contains("id(\"io.github.huyz0.os.biningester.java-conventions\")");
        }
        assertThat(scripts).as("the premise: settings' modules are read").hasSizeGreaterThan(8);
        // ⚠️ AND NOWHERE ELSE: a task that set it again could set it "false".
        assertThat(scripts.stream().mapToLong(script -> Pattern.compile(Pattern.quote(
                "junit.jupiter.execution.timeout.threaddump.enabled")).matcher(script)
                .results().count()).sum()).as("set in exactly the two blocks").isEqualTo(2);
    }

    /**
     * ⚠️ A TEST TASK RUNS, NEVER SERVED FROM THE BUILD CACHE (M13.62): CI
     * restores the build cache so compiles are skipped, and a test result
     * taken from it would be a pass nobody ran. Set by construction, like the
     * thread dump, in the conventions plugin and in buildSrc's own build.
     */
    @Test
    void everyTestTaskRunsRatherThanComingFromTheBuildCache() throws Exception {
        for (String path : List.of("buildSrc/src/main/kotlin/"
                + "io.github.huyz0.os.biningester.java-conventions.gradle.kts",
                "buildSrc/build.gradle.kts")) {
            assertThat(withTypeTestBlocks(code(path))).as(path).anyMatch(block ->
                    topLevel(block).contains("outputs.cacheIf(")
                            && block.replaceAll("\\s+", " ").contains(") { false }"));
        }
    }

    /** {@code block} without the bodies of the blocks nested inside it. */
    private static String topLevel(String block) {
        StringBuilder top = new StringBuilder();
        int depth = 0;
        for (char c : block.toCharArray()) {
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            } else if (depth <= 1) {
                top.append(c);
            }
        }
        return top.toString();
    }

    /** The bodies of the script's {@code tasks.withType<Test>().configureEach { .. }} blocks. */
    private static List<String> withTypeTestBlocks(String script) {
        List<String> blocks = new java.util.ArrayList<>();
        String opener = "tasks.withType<Test>().configureEach {";
        for (int at = script.indexOf(opener); at >= 0; at = script.indexOf(opener, at + 1)) {
            int depth = 0;
            int end = at + opener.length() - 1;
            for (int i = end; i < script.length(); i++) {
                char c = script.charAt(i);
                if (c == '{') {
                    depth++;
                } else if (c == '}' && --depth == 0) {
                    end = i;
                    break;
                }
            }
            blocks.add(script.substring(at, end + 1));
        }
        return blocks;
    }

    private static void write(Path root, String relative, String text) throws Exception {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, text);
    }

    private static void git(Path repo, String... args) throws Exception {
        List<String> command = new java.util.ArrayList<>(List.of("git"));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).directory(repo.toFile())
                .redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        assertThat(process.waitFor()).as("git " + String.join(" ", args) + ": " + output)
                .isZero();
    }

    /** A repository file as code: CRLF normalised, comments stripped. */
    private static String code(String relative) throws Exception {
        return LegacyScriptQuarantine.INSTANCE.code(
                Files.readString(repository().resolve(relative)).replace("\r\n", "\n"));
    }

    /** The checkout's root: the harness runs with {@code buildSrc} as its working directory. */
    private static Path repository() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve(".pre-commit-config.yaml"))) {
            current = current.getParent();
        }
        return current;
    }
}
