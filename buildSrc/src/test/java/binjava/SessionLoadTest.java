// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What EVERY session loads, whether or not any work happens.
 *
 * <p>⚠️ MEASURED against the harness this one was distilled from: pstore's
 * always-loaded set is 10,053 bytes and this project's was 26,856 — AGENTS.md
 * alone 20,936 against pstore's 6,664, with the excess being incident narrative
 * rather than index. Two blind-spot paragraphs on check-tdd, a seven-row "what CI
 * can and cannot enforce" table, the `--no-verify` caveat: all true, all worth
 * keeping, none of them an index, and all of them paid for on every session that
 * never touches a gate.
 *
 * <p>AGENTS.md says of itself that it "is deliberately an index". That sentence
 * is prose in the file nobody gates — the same defect {@link BacklogBoundsTest}
 * records for the backlog's "current milestone only", and the same remedy.
 *
 * <p>⚠️ THE SET IS DERIVED FROM CLAUDE.md's IMPORTS, not listed in the gate. Rung
 * 2 of the gate-design ladder rather than rung 3: a hardcoded list would keep
 * saying 26,856 the day someone adds a third import, which is precisely the
 * failure mode of a hand-maintained list of what runs.
 *
 * <p>⚠️ A CAP IS NOT A CONTENT RULE. It cannot tell an index from an essay; it
 * makes the tradeoff VISIBLE, so adding a paragraph to layer 0 means deciding
 * what leaves it. Detail moves DOWN a layer — to a standard, loaded when a skill
 * says to read it — and is never deleted to make a number go green.
 */
class SessionLoadTest {

  private record Gate(int status, String output) {
  }

  private Gate gate(Path dir) throws Exception {
    Process p = new ProcessBuilder(List.of("bash", "scripts/check-session-load.sh"))
        .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Gate(p.waitFor(), out);
  }

  /**
   * A scratch repository whose backlog is already valid, so only layer 0 is
   * under test.
   *
   * <p>⚠️ A FRESH SUBDIRECTORY per arm. Both arms of a two-sided test run in one
   * {@code @TempDir}, and the second one materialising over the first fails on
   * the copy rather than measuring anything.
   */
  private Path scratch(Path parent, String name, String claudeMd, int agentsBytes,
      int skillsBytes) throws Exception {
    Path dir = parent.resolve(name);
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-session-load.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/product"));
    Files.writeString(dir.resolve("docs/internal/product/backlog.md"),
        "| ID | Task | Serves | State |\n|---|---|---|---|\n| M9.1 | s | — | todo |\n");
    Files.writeString(dir.resolve("CLAUDE.md"), claudeMd);
    Files.writeString(dir.resolve("AGENTS.md"), "x".repeat(agentsBytes));
    Files.createDirectories(dir.resolve(".agents/skills"));
    Files.writeString(dir.resolve(".agents/skills/README.md"), "x".repeat(skillsBytes));
    return dir;
  }

  /**
   * ⚠️ Both sides of the boundary, because a gate that refuses everything and a
   * gate that refuses the right thing are indistinguishable from one arm.
   */
  @Test
  void anOversizedSessionLoadIsRefusedAndOneWithinTheCapIsNot(@TempDir Path dir) throws Exception {
    Gate bad = gate(scratch(dir, "over", "@AGENTS.md\n", 40_000, 100));
    assertThat(bad.status())
        .as("layer 0 is paid on every session, including the ones that do no work\n%s",
            bad.output())
        .isNotZero();
    assertThat(bad.output()).contains("AGENTS.md");

    Gate good = gate(scratch(dir, "under", "@AGENTS.md\n", 4_000, 100));
    assertThat(good.status())
        .as("an index still has to be able to say what it indexes\n%s", good.output())
        .isZero();
  }

  /**
   * Layer 1 is every skill's {@code description:} line, and only that line: it is
   * what an agent sees before deciding whether to load a skill's body.
   *
   * <p>⚠️ MEASURED: 3,034 bytes across 13 skills here against pstore's 1,011
   * across 5 — the count differs more than the length, but a description is a
   * *when to use this*, not a summary of the skill, and eight of the thirteen had
   * grown past 200 characters into the second.
   *
   * <p>⚠️ Both sides again: over the cap refuses and names the skill, under it
   * passes. A description still has to say when the skill applies.
   */
  @Test
  void anOversizedSkillDescriptionIsRefusedAndOneWithinTheCapIsNot(@TempDir Path dir)
      throws Exception {
    Path over = scratch(dir, "desc-over", "@AGENTS.md\n", 100, 100);
    skill(over, "verbose", "d".repeat(400));
    Gate bad = gate(over);
    assertThat(bad.status())
        .as("layer 1 is paid on every session, for every skill, loaded or not\n%s",
            bad.output())
        .isNotZero();
    assertThat(bad.output()).contains("verbose");

    Path under = scratch(dir, "desc-under", "@AGENTS.md\n", 100, 100);
    skill(under, "terse", "Use when the thing happens.");
    Gate good = gate(under);
    assertThat(good.status())
        .as("a description still has to say when the skill applies\n%s", good.output())
        .isZero();
  }

  /** A skill with a frontmatter description, as every skill has. */
  private void skill(Path dir, String name, String description) throws Exception {
    Path f = dir.resolve(".agents/skills").resolve(name).resolve("SKILL.md");
    Files.createDirectories(f.getParent());
    Files.writeString(f, "---\nname: " + name + "\ndescription: " + description + "\n---\n\n# "
        + name + "\n");
  }

  /**
   * ⚠️ A file becomes layer 0 by being IMPORTED, so the gate must count what
   * CLAUDE.md actually pulls in and nothing else.
   *
   * <p>The same two files, the same bytes, differing only in whether the second
   * is imported: over the cap when it is, under when it is not. A gate with a
   * hardcoded list passes both arms and measures nothing.
   */
  @Test
  void onlyTheFilesClaudeMdIMPORTSAreCounted(@TempDir Path dir) throws Exception {
    Gate both =
        gate(scratch(dir, "both", "@AGENTS.md\n@.agents/skills/README.md\n",
            10_000, 10_000));
    assertThat(both.status())
        .as("two imports of 10,000 bytes are 20,000 bytes of every session\n%s", both.output())
        .isNotZero();

    Gate one = gate(scratch(dir, "one", "@AGENTS.md\n", 10_000, 10_000));
    assertThat(one.status())
        .as("the same file on disk, no longer imported, is no longer a session cost\n%s",
            one.output())
        .isZero();
  }
}
