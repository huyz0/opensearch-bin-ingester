// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A rule cited by NUMBER names nothing and goes stale silently.
 *
 * <p>⚠️ "review.md rule 12" appears in scripts, skills, tests and backlog rows,
 * and resolving it means loading a 14.8 KB standard to learn which rule that is.
 * It is the same defect AGENTS.md already forbids one level out — *never report
 * to a person in bare task IDs* — applied to rules instead of tasks, and pstore,
 * the harness this one was distilled from, made it a non-negotiable after the
 * same thing bit it: "cite these by name, never by number. The numbering is for
 * reading order and shifts when a rule is added — a numbered cross-reference in
 * another file goes stale silently. It already did once."
 *
 * <p>⚠️ THE NUMBERING IS ALREADY UNDER STRAIN HERE: review.md carries 1a, 1b and
 * 3a because inserting a rule would have renumbered every citation in the tree.
 * A name has no such cost.
 *
 * <p>Two predicates, and the second is what makes the first worth having:
 *
 * <ul>
 *   <li>A numeric citation is refused. Names are not optional decoration.
 *   <li>A named citation must RESOLVE to a rule that exists — otherwise a name
 *       is just a number with better spelling.
 * </ul>
 *
 * <p>⚠️ THE ARCHIVE IS NOT JUDGED. backlog-done.md and backlog-notes.md hold rows
 * verbatim as they were written; rewriting them to satisfy a citation style would
 * turn this gate into an instruction to edit the record, and nothing reads them
 * as instructions.
 */
class RuleCitationTest {

  private record Gate(int status, String output) {
  }

  private Path scratch(Path parent, String name) throws Exception {
    Path dir = parent.resolve(name);
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : List.of("check-rule-citations.sh", "lib.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.createDirectories(dir.resolve("docs/internal/standards"));
    Files.writeString(dir.resolve("docs/internal/standards/review.md"),
        "# Review\n\n"
            + "1. **Two agents review it** (`two-reviewers`) — both mandatory.\n"
            + "12. **Two rounds is the cap** (`two-round-cap`) — round one finds.\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    return dir;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = new ProcessBuilder("bash", "-c", script)
        .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  private void cite(Path dir, String rel, String text) throws Exception {
    Path p = dir.resolve(rel);
    Files.createDirectories(p.getParent());
    Files.writeString(p, text);
    run(dir, "git add -A");
  }

  private Gate gate(Path dir) throws Exception {
    Process p = new ProcessBuilder(List.of("bash", "scripts/check-rule-citations.sh"))
        .directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Gate(p.waitFor(), out);
  }

  /** A number names nothing; the same citation by name is what the gate wants. */
  @Test
  void aNumericCitationIsRefusedAndTheSameOneByNameIsNot(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "numeric");
    cite(d, "scripts/thing.sh", "# implements review.md rule 12, somehow\n");
    Gate bad = gate(d);
    assertThat(bad.status()).as("resolving it means loading a 14.8 KB standard\n%s",
        bad.output()).isNotZero();
    assertThat(bad.output()).contains("rule 12");

    cite(d, "scripts/thing.sh", "# implements review.md rule two-round-cap, somehow\n");
    Gate good = gate(d);
    assertThat(good.status()).as("a name says what the rule IS\n%s", good.output()).isZero();
  }

  /** ⚠️ A name that resolves to nothing is a number with better spelling. */
  @Test
  void aNamedCitationThatResolvesToNoRuleIsRefused(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "unresolved");
    cite(d, "scripts/thing.sh", "# implements review.md rule three-round-cap\n");
    Gate bad = gate(d);
    assertThat(bad.status())
        .as("an unresolvable name is the bare-task-ID defect one step worse: it names "
            + "nothing AND there is nothing to look up\n%s", bad.output())
        .isNotZero();
    assertThat(bad.output()).contains("three-round-cap");
  }

  /**
   * ⚠️ The archive holds rows verbatim as they were written, and is judged by
   * nothing — including this.
   */
  @Test
  void aNumericCitationInTheBacklogARCHIVEIsNotJudged(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "archive");
    cite(d, "docs/internal/product/backlog-done.md",
        "| M1.1 | argued under review.md rule 12 | — | **done** |\n");
    Gate g = gate(d);
    assertThat(g.status())
        .as("a style gate that edits the historical record is not a style gate\n%s", g.output())
        .isZero();

    cite(d, "docs/internal/product/backlog.md",
        "| M1.2 | argued under review.md rule 12 | — | todo |\n");
    Gate live = gate(d);
    assertThat(live.status())
        .as("the LIVE backlog is an instruction and is judged like one\n%s", live.output())
        .isNotZero();
  }
}
