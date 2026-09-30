// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * M13.8 (M12 harvest R6): the tests that drive shell and Python scripts under
 * {@code scripts/} are quarantined out of {@code ./gradlew -p buildSrc test}
 * into {@code legacyScriptTest} by {@code buildSrc/legacy-script-tests.txt},
 * so the task is green on the development rig, where those scripts do not
 * run. They are kept, and runnable by hand: no gate runs
 * {@code legacyScriptTest}.
 *
 * <p>⚠️ THE LIST IS REFUSED ANYTHING ELSE ({@link LegacyScriptQuarantine}): a
 * wildcard, a {@code nativeGateTest} member, a missing class, a class whose
 * code names no script -- each a hole a review round found open -- and the
 * build script may take the list from nowhere but that file. It does not tell
 * a retired script from a live one (M13.8a review P1).
 */
class LegacyScriptQuarantineTest {

    private static final String PACKAGE = "io.github.huyz0.os.biningester.";
    private static final Path TESTS =
            repository().resolve("buildSrc/src/test/java/io/github/huyz0/os/biningester");

    @Test
    void everyQuarantinedEntryIsALegacyScriptTest() throws Exception {
        List<String> entries = LegacyScriptQuarantine.INSTANCE.entries(
                Files.readString(repository().resolve("buildSrc/legacy-script-tests.txt")));
        assertThat(entries).as("the quarantine file's entries").isNotEmpty();

        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(entries, nativeGateTests(),
                LegacyScriptQuarantineTest::source)).as("the quarantine file").isEmpty();
    }

    @Test
    void aNativeGateTestIsRefused() throws Exception {
        Set<String> natives = nativeGateTests();
        assertThat(natives).as("the premise: nativeGateTest's members are read")
                .contains("GradleGateWiringTest");

        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("GradleGateWiringTest"),
                natives, LegacyScriptQuarantineTest::source))
                .as("a native gate test, although its code names scripts/")
                .singleElement().asString().contains("nativeGateTest");
    }

    @Test
    void aWildcardEntryIsRefused() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("Wired*"), Set.of(),
                name -> "class X { String s = \"scripts/check-wired.sh\"; }"))
                .as("⚠️ A WILDCARD: the filter would widen it to classes nobody checked")
                .singleElement().asString().contains("not a plain class name");
    }

    @Test
    void aClassThatDrivesNoScriptIsRefused() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("Plain"), Set.of(),
                name -> "class Plain { void run() { } }"))
                .singleElement().asString().contains("drives no script under scripts/");
    }

    @Test
    void aScriptNamedOnlyInACommentDoesNotCount() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("Prose"), Set.of(),
                name -> "/** Once drove scripts/check-module.sh. */\n"
                        + "class Prose { // scripts/lib.sh\n void run() { } }"))
                .as("⚠️ COMMENTS STRIPPED: prose naming a script is not a script test")
                .singleElement().asString().contains("drives no script under scripts/");
    }

    /**
     * ⚠️ THE WHOLE DECLARATION AND THE DEFAULT TASK's FILTER, EXACTLY (M13.8
     * review T6): an unanchored check let {@code + listOf("X")} after the
     * declaration hide a class the guard never saw. And no other exclusion
     * anywhere in the script, so the list is the only way out of the default
     * task (review P7).
     */
    @Test
    void theBuildScriptTakesTheListFromTheFileAloneAndFiltersExactly() throws Exception {
        String build = buildScript();
        assertThat(build).as("the declaration and the default task's filter, exactly")
                .contains("val legacyScriptTests = file(\"legacy-script-tests.txt\").readLines().map { it.trim() }\n    .filter { it.isNotEmpty() && !it.startsWith(\"#\") }\n\ntasks.named<Test>(\"test\") {\n    filter {\n        legacyScriptTests.forEach { excludeTestsMatching(\"io.github.huyz0.os.biningester.$it\") }\n    }\n}\n");
        assertThat(Pattern.compile("\\blegacyScriptTests\\b").matcher(build).results().count())
                .as("declared once, used by the two filters").isEqualTo(3);
        assertThat(Pattern.compile("\\bexclude\\w*").matcher(build).results().count())
                .as("⚠️ NO OTHER EXCLUSION: the list's excludeTestsMatching is the"
                        + " only exclude of any kind -- no exclude { }, excludeTags,"
                        + " excludePatterns (M13.8a review P2/T2)").isEqualTo(1);
        assertThat(build).as("the quarantined task runs every entry, exactly")
                .contains("tasks.register<Test>(\"legacyScriptTest\")")
                .contains("legacyScriptTests.forEach { includeTestsMatching(\""
                        + PACKAGE + "$it\") }");
        String nativeTask = build.substring(build.indexOf(
                "tasks.register<Test>(\"nativeGateTest\")"));
        assertThat(nativeTask).as("⚠️ AND THE GUARD's TASK RERUNS WHEN EITHER FILE CHANGES")
                .contains("inputs.file(\"build.gradle.kts\")")
                .contains("inputs.file(\"legacy-script-tests.txt\")");
    }

    /**
     * ⚠️ EVERY ENTRY, NOT THE FIRST (M13.8a review T1): bad entries among good
     * ones are refused, by name, and the good ones are not.
     */
    @Test
    void everyEntryIsCheckedAndOnlyTheBadOnesAreRefused() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(
                List.of("Good", "Wired*", "Native", "Plain", "Ghost", "Also"), Set.of("Native"),
                name -> switch (name) {
                    case "Good", "Also", "Native" -> "class X { String s = \"scripts/check-x.sh\"; }";
                    case "Plain" -> "class Plain { }";
                    default -> null;
                }))
                .containsExactly(
                        "Wired* is not a plain class name",
                        "Native is a nativeGateTest member",
                        "Plain drives no script under scripts/",
                        "Ghost does not exist");
    }

    @Test
    void aClassThatDoesNotExistIsRefused() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("Ghost"), Set.of(),
                name -> null))
                .as("⚠️ NO SOURCE TO READ: a name whose code nobody could check (review T7)")
                .singleElement().asString().contains("does not exist");
    }

    @Test
    void aScriptsPathThatIsNoScriptDoesNotCount() {
        assertThat(LegacyScriptQuarantine.INSTANCE.refusals(List.of("Docs"), Set.of(),
                name -> "class Docs { String s = \"scripts/README.md\"; }"))
                .as("⚠️ ONLY A .sh OR .py IS A RETIRED SCRIPT (review T8)")
                .singleElement().asString().contains("drives no script under scripts/");
    }

    /** buildSrc's build script, its line endings normalised (a Windows checkout has CRLF). */
    private static String buildScript() throws java.io.IOException {
        // ⚠️ AS CODE: comments stripped, so a commented-out line does not
        // count as the line it was.
        return LegacyScriptQuarantine.INSTANCE.code(
                Files.readString(repository().resolve("buildSrc/build.gradle.kts"))
                        .replace("\r\n", "\n"));
    }

    /** A buildSrc test class's source by its simple name, or null. */
    private static String source(String name) {
        Path file = TESTS.resolve(name + ".java");
        try {
            return Files.exists(file) ? Files.readString(file) : null;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** The class names {@code nativeGateTest} includes. */
    private static Set<String> nativeGateTests() throws Exception {
        String build = buildScript();
        int start = build.indexOf("tasks.register<Test>(\"nativeGateTest\")");
        assertThat(start).as("the nativeGateTest task").isNotNegative();
        Matcher m = Pattern.compile("includeTestsMatching\\(\"" + Pattern.quote(PACKAGE)
                + "([A-Za-z0-9]+)\"\\)").matcher(build.substring(start));
        Set<String> names = new LinkedHashSet<>();
        while (m.find()) {
            names.add(m.group(1));
        }
        return names;
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
