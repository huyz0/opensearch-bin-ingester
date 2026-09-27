// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code scripts/mutants.py}'s UNUSED refusal of a baseline entry, through the
 * same production door and scratch repository as {@link MutantsGateTest}
 * (M10.7).
 *
 * <p>⚠️ AN ENTRY IS UNUSED ONLY WHEN THE DIFF MOVED OR CHANGED ITS LINE. A
 * diff-scoped run mutates only changed lines, so a mutated method with the
 * key absent is not proof the key moved: an edit to another line of the
 * method leaves the baselined line unexamined, and refusing that entry
 * refused a correct excuse. Split out of {@link MutantsGateTest} when these
 * cases took it past the 700-line limit.
 */
class MutantsBaselineUnusedTest {

  private final MutantsGateTest gates = new MutantsGateTest();

  /**
   * ⚠️ The other way an entry stops describing anything, and the one the
   * staleness check alone cannot see: the METHOD was edited, so every line
   * number and ordinal in it moved and the recorded key matches no mutant at
   * all. Kept, it is a standing excuse for whatever lands on that key next.
   * The method IS mutated this run, which is what distinguishes it from an
   * entry that is simply outside the diff.
   */
  @Test
  void aBaselineEntryWhoseKeyHasMovedIsRefusedAsUnused(@TempDir Path dir) throws Exception {
    // ⚠️ THE KEY REALLY MOVES (M10.7): seven lines are added above line 34, so
    // what the entry excused now sits at line 41 under a different key.
    Path repo = baselinedOn(dir, 34, source(45), withLinesAddedAtTop(source(45), 7),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#0", "KILLED"),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#1", "KILLED"));

    String out = gates.gate(repo);

    assertThat(out).as("the score is 100%% and the gate STILL refuses%n%s", out).startsWith("1");
    assertThat(out).as(out).contains("UNUSED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /**
   * ⚠️ M10.7, harvested from 5f7e242's review: a diff-scoped run mutates only
   * the CHANGED lines, so an edit to ANOTHER line of the method yields mutants
   * in it without the baselined line ever being examined. The entry still
   * names the mutant it excused, and refusing it refused a correct excuse --
   * which the case above used to lock in.
   */
  @Test
  void anEditToAnotherLineOfTheMethodKeepsItsBaselineEntry(@TempDir Path dir)
      throws Exception {
    // ⚠️ THE LINES EITHER SIDE, so the boundary of "its own line" is pinned on
    // the side this task fixes: an off-by-one there refuses exactly these.
    Path repo = baselinedOn(dir, 34, source(45),
        withLineChanged(withLineChanged(source(45), 33), 35),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::33::MATH#0", "KILLED"),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::35::MATH#0", "KILLED"));

    String out = gates.gate(repo);

    assertThat(out).as("line 34 neither changed nor moved%n%s", out).startsWith("0");
    assertThat(out).as(out).doesNotContain("UNUSED");
  }

  /** The same, measured over a CI range rather than the index. */
  @Test
  void anEditToAnotherLineKeepsItsBaselineEntryOverACheckRange(@TempDir Path dir)
      throws Exception {
    Path repo = baselinedOn(dir, 34, source(45), withLineChanged(source(45), 41),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#0", "KILLED"));
    gates.run(repo, "git commit -qm change");

    String out = gates.gate(repo, "HEAD~1");

    assertThat(out).as("the range's diff, not the empty index%n%s", out).startsWith("0");
    assertThat(out).as(out).doesNotContain("UNUSED");
  }

  /**
   * The user's git config does not decide the parse: with prefixes turned off
   * or renamed, and colour forced, the diff is still read.
   */
  @Test
  void anEditToAnotherLineKeepsItsBaselineEntryWhateverTheDiffConfig(@TempDir Path dir)
      throws Exception {
    Path repo = baselinedOn(dir, 34, source(45), withLineChanged(source(45), 41),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::41::MATH#0", "KILLED"));
    gates.run(repo, "git config diff.noprefix true && git config diff.mnemonicPrefix true"
        + " && git config color.diff always");

    String out = gates.gate(repo);

    assertThat(out).as("read whatever the config says%n%s", out).startsWith("0");
    assertThat(out).as(out).doesNotContain("UNUSED");
  }

  /** Lines REMOVED above the entry move it as surely as lines added. */
  @Test
  void aBaselineEntryWithLinesRemovedAboveItIsRefusedAsUnused(@TempDir Path dir)
      throws Exception {
    Path repo = baselinedOn(dir, 34, source(45), withLinesRemoved(source(45), 20, 3),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::20::MATH#0", "KILLED"));

    String out = gates.gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("UNUSED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /** And ONE line added mid-method, not only a block at the top of the file. */
  @Test
  void aBaselineEntryWithOneLineAddedAboveItIsRefusedAsUnused(@TempDir Path dir)
      throws Exception {
    Path repo = baselinedOn(dir, 34, source(45), withLineAddedAfter(source(45), 20),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::21::MATH#0", "KILLED"));

    String out = gates.gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("UNUSED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /** The entry's OWN line changed and no longer yields that mutant: it is gone. */
  @Test
  void aBaselineEntryWhoseOwnLineChangedIsRefusedAsUnused(@TempDir Path dir) throws Exception {
    Path repo = baselinedOn(dir, 34, source(45), withLineChanged(source(45), 34),
        MutantsGateTest.mutant("io.github.huyz0.os.biningester.X::f()J::34::MATH#0", "KILLED"));

    String out = gates.gate(repo);

    assertThat(out).as(out).startsWith("1");
    assertThat(out).as(out).contains("UNUSED").contains("io.github.huyz0.os.biningester.X::f()J::34::MATH#1");
  }

  /** {@code lines} numbered lines of Java source, so a diff has lines to move. */
  private static String source(int lines) {
    StringBuilder b = new StringBuilder();
    for (int i = 1; i <= lines; i++) {
      b.append("  long l").append(i).append(" = ").append(i).append(";\n");
    }
    return b.toString();
  }

  private static String withLinesAddedAtTop(String source, int added) {
    return "  // added\n".repeat(added) + source;
  }

  private static String withLinesRemoved(String source, int from, int count) {
    List<String> lines = new java.util.ArrayList<>(List.of(source.split("\n")));
    lines.subList(from - 1, from - 1 + count).clear();
    return String.join("\n", lines) + "\n";
  }

  private static String withLineAddedAfter(String source, int line) {
    List<String> lines = new java.util.ArrayList<>(List.of(source.split("\n")));
    lines.add(line, "  long added = 0;");
    return String.join("\n", lines) + "\n";
  }

  private static String withLineChanged(String source, int line) {
    List<String> lines = new java.util.ArrayList<>(List.of(source.split("\n")));
    lines.set(line - 1, lines.get(line - 1) + " // changed");
    return String.join("\n", lines) + "\n";
  }

  /**
   * {@code X.java} committed as {@code base}, staged as {@code change}, with
   * {@code X::f()J::<line>::MATH#1} baselined and the report holding {@code mutants}.
   */
  private Path baselinedOn(Path dir, int line, String base, String change, String... mutants)
      throws Exception {
    Path repo = gates.scratch(dir);
    gates.module(repo, "format");
    Path x = repo.resolve("format/src/main/java/io/github/huyz0/os/biningester/X.java");
    Files.writeString(x, base);
    gates.fixture(repo, "format", MutantsGateTest.report(mutants));
    Files.createDirectories(repo.resolve("baselines"));
    Files.writeString(repo.resolve("baselines/mutants.txt"),
        "io.github.huyz0.os.biningester.X::f()J::" + line
            + "::MATH#1  equivalent mutant on a defensive branch; M8.62\n");
    gates.commit(repo);
    Files.writeString(x, change);
    gates.run(repo, "git add -A");
    return repo;
  }

}
