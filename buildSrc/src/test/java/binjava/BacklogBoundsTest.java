// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The backlog is read whole at the start of every session, so its size is a
 * per-session tax and nothing was collecting it.
 *
 * <p>⚠️ MEASURED: 18,638 bytes on 2026-08-31, 315,383 on 2026-09-05 — monotonic,
 * seventeen-fold, five days. Of that, 71,149 bytes were preamble before the first
 * heading, 84,501 were the rows of M1, M2 and M3 (all complete), and the longest
 * single row was 18,845 bytes. backlog.md's own first line says "current
 * milestone only", which has been false since M2 closed. That sentence is prose
 * in the one file nobody gates — rung 7 of the gate-design ladder for a rule that
 * is a predicate over the file.
 *
 * <p>Three predicates, and each test carries its own negative control, because a
 * bound with no counter-case is indistinguishable from a gate that refuses
 * everything:
 *
 * <ul>
 *   <li>A row that is <b>done</b> belongs in the archive, not the backlog.
 *   <li>A row is a <b>summary</b>. The essay moves to backlog-notes.md, which is
 *       opened for the ONE task a session picks rather than for all of them.
 *   <li>The <b>preamble</b> is bounded: it is the part that grows without anyone
 *       adding a task.
 * </ul>
 *
 * <p>⚠️ THE ARCHIVE IS NOT JUDGED. Applying the row cap to it would mean
 * rewriting rows that already shipped, which is how a size gate turns into an
 * instruction to destroy the record — and the archive is not loaded by anything,
 * so its size costs nothing.
 *
 * <p>⚠️ AND EVERY ID MUST STILL RESOLVE. check-commit-msg.sh binds the commit
 * subject to a real task; moving done rows out of backlog.md would otherwise make
 * every completed task's ID unnameable, so the ID lookup spans the backlog AND
 * its archive.
 */
class BacklogBoundsTest {

  private static final String HDR = "| ID | Task | Serves | State |\n|---|---|---|---|\n";

  private void write(Path dir, String rel, String body) throws Exception {
    Path p = dir.resolve(rel);
    Files.createDirectories(p.getParent());
    Files.writeString(p, body);
  }

  /** A scratch repository holding the gate and a backlog. */
  private Path scratch(Path dir, String... scripts) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Files.createDirectories(dir.resolve("scripts"));
    for (String f : scripts) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Path dst = dir.resolve("scripts/lib.sh");
    if (!Files.exists(dst)) {
      Files.copy(repo.resolve("scripts/lib.sh"), dst);
    }
    return dir;
  }

  private record Gate(int status, String output) {
  }

  private Gate run(Path dir, List<String> cmd) throws Exception {
    Process p = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Gate(p.waitFor(), out);
  }

  private Gate gate(Path dir) throws Exception {
    return run(dir, List.of("bash", "scripts/check-backlog-size.sh"));
  }

  private Gate commitMsg(Path dir, String subject) throws Exception {
    Files.writeString(dir.resolve("msg.txt"), subject + "\n");
    return run(dir, List.of("bash", "scripts/check-commit-msg.sh", "msg.txt"));
  }

  /** A done row belongs in the archive; the archive is not judged by the same rule. */
  @Test
  void aDoneRowInTheBacklogIsRefusedAndTheSameRowInTheArchiveIsNot(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir, "check-backlog-size.sh");
    write(d, "docs/internal/product/backlog.md", HDR + "| M9.1 | short | — | **done** |\n");
    write(d, "docs/internal/product/backlog-done.md", HDR);
    Gate bad = gate(d);
    assertThat(bad.status()).as("a done row is dead weight in a file read every session\n%s",
        bad.output()).isNotZero();

    write(d, "docs/internal/product/backlog.md", HDR + "| M9.2 | short | — | todo |\n");
    write(d, "docs/internal/product/backlog-done.md", HDR + "| M9.1 | short | — | **done** |\n");
    Gate good = gate(d);
    assertThat(good.status()).as("the archive is where done rows are SUPPOSED to be\n%s",
        good.output()).isZero();
  }

  /** A row is a summary, and the boundary is checked from both sides. */
  @Test
  void aRowOverTheLengthCapIsRefusedAndOneUnderItIsNot(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "check-backlog-size.sh");
    write(d, "docs/internal/product/backlog-done.md", HDR);
    write(d, "docs/internal/product/backlog.md",
        HDR + "| M9.1 | " + "e".repeat(900) + " | — | todo |\n");
    Gate bad = gate(d);
    assertThat(bad.status()).as("the longest row in the real file was 18,845 bytes\n%s",
        bad.output()).isNotZero();
    assertThat(bad.output()).contains("M9.1");

    write(d, "docs/internal/product/backlog.md",
        HDR + "| M9.1 | " + "e".repeat(100) + " | — | todo |\n");
    Gate good = gate(d);
    assertThat(good.status()).as("a summary row must be allowed to say something\n%s",
        good.output()).isZero();
  }

  /**
   * ⚠️ The preamble is the part that grows with nobody adding a task, so it is
   * bounded separately — the row rules cannot see it at all.
   */
  @Test
  void aPreambleOverTheCapIsRefusedAndAShortOneIsNot(@TempDir Path dir) throws Exception {
    Path d = scratch(dir, "check-backlog-size.sh");
    write(d, "docs/internal/product/backlog-done.md", HDR);
    write(d, "docs/internal/product/backlog.md",
        "# Backlog\n\n" + "narrative. ".repeat(900) + "\n\n" + HDR + "| M9.1 | s | — | todo |\n");
    Gate bad = gate(d);
    assertThat(bad.status()).as("71,149 bytes of it accumulated before the first heading\n%s",
        bad.output()).isNotZero();

    write(d, "docs/internal/product/backlog.md",
        "# Backlog\n\nOpen rows for the current milestone.\n\n" + HDR
            + "| M9.1 | s | — | todo |\n");
    Gate good = gate(d);
    assertThat(good.status()).as("a backlog is allowed to explain itself briefly\n%s",
        good.output()).isZero();
  }

  /**
   * ⚠️ Moving done rows out must not make their IDs unnameable: check-commit-msg
   * looks in the backlog AND the archive, and still refuses an ID in neither.
   */
  @Test
  void anIdThatMovedToTheArchiveStillResolvesButAnInventedOneDoesNot(@TempDir Path dir)
      throws Exception {
    Path d = scratch(dir, "check-commit-msg.sh");
    write(d, "docs/internal/product/backlog.md", HDR + "| M9.2 | short | — | todo |\n");
    write(d, "docs/internal/product/backlog-done.md",
        HDR + "| M9.1 | short | — | **done** |\n");
    Gate archived = commitMsg(d, "M9.1 a follow-up to a task that already landed");
    assertThat(archived.status())
        .as("a completed task's ID must stay nameable, or every fix-up commit is refused\n%s",
            archived.output())
        .isZero();

    Gate invented = commitMsg(d, "M9.9 an ID nobody ever wrote down");
    assertThat(invented.status())
        .as("an ID in neither file names nothing, which is the defect the gate exists for\n%s",
            invented.output())
        .isNotZero();
  }
}
