// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Every decision-record citation in this tree has a file behind it.
 *
 * <p>⚠️ AN UNRESOLVABLE ID NAMES NOTHING <b>AND</b> LEAVES NOTHING TO LOOK UP,
 * which AGENTS.md's Never list states in as many words about {@code M<n>} and
 * {@code R<n>}. Decision records were outside that rule and outside every gate:
 * {@code check-links.sh} judges relative markdown links, and a citation written
 * into a javadoc is not one.
 *
 * <p>⚠️ MEASURED, WHICH IS WHY THIS EXISTS. M4 was delivered onto {@code main}
 * from the {@code archive/m4} branch and its ELEVEN decision records were not:
 * numbers 0027 through 0037 existed only on the branch that was left behind,
 * while the code that arrived cited nine of them <b>136 times</b>. The
 * most-cited was the record justifying the {@code (podId, incarnationId,
 * flushSeq)} idempotency key that M4.10, M5.1 and M5.23 are all built on — 46
 * citations with nothing to read.
 *
 * <p>⚠️ NO CITATION IS SPELT OUT IN THIS FILE, and that is forced rather than
 * stylistic: the gate reads every tracked file, this one included, so an
 * example written in the citation form would BE a citation — and one naming a
 * record that does not exist would make the gate refuse this repository for its
 * own test fixture. Review measured exactly that on the first draft.
 */
class AdrRefsGateTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final String PREFIX = "ADR" + "-";
  private static final String DECISIONS = "docs/internal/product/decisions";

  private record Run(int exit, String out) {
  }

  private static Run run(Path dir, String... command) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(command).directory(dir.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Run(p.waitFor(), out);
  }

  /**
   * ⚠️ SCRATCH UNDER {@code build/tmp}, never the system temp directory
   * (testing.md rule 17). An earlier draft used {@code @TempDir} with
   * {@code CleanupMode.NEVER} and no discard, which leaked one git repository
   * per harness run into {@code /tmp} — measured, 23 already present.
   */
  private static Path scratch(String name) throws Exception {
    Path dir = ROOT.resolve("buildSrc/build/tmp/adr-refs")
        .resolve(name + "-" + UUID.randomUUID());
    Files.createDirectories(dir);
    return dir;
  }

  /**
   * A repository holding one real record, one citation that resolves, and
   * whatever else the caller writes.
   *
   * <p>⚠️ THE GATE ITSELF IS COPIED IN, so the fixture must also hold the
   * records the GATE's own text cites — it does not cite any, deliberately, and
   * this comment is what keeps that true if it ever starts to.
   */
  private static Path fixture(String name) throws Exception {
    Path repo = scratch(name);
    Files.createDirectories(repo.resolve("scripts"));
    Files.createDirectories(repo.resolve(DECISIONS));
    Files.createDirectories(repo.resolve("src"));
    Files.copy(ROOT.resolve("scripts/lib.sh"), repo.resolve("scripts/lib.sh"));
    Files.copy(ROOT.resolve("scripts/check-adr-refs.sh"),
        repo.resolve("scripts/check-adr-refs.sh"));
    repo.resolve("scripts/check-adr-refs.sh").toFile().setExecutable(true);
    Files.writeString(repo.resolve(DECISIONS + "/0001-a-real-one.md"), "# 0001. A real one\n");
    Files.writeString(repo.resolve("notes.md"),
        "This cites " + PREFIX + "0001, which exists.\n");
    run(repo, "git", "init", "-q");
    run(repo, "git", "config", "user.email", "t@example.com");
    run(repo, "git", "config", "user.name", "t");
    return repo;
  }

  /**
   * ⚠️ THE REAL TREE, not a fixture. A gate proved correct against scratch
   * repositories and never run here is a gate that has never met the corpus it
   * is for — and the 136 dangling citations that motivated it were here.
   *
   * <p>⚠️ THE COUNT IS ASSERTED, NOT JUST THE EXIT STATUS. Exit 0 is what a gate
   * that read nothing returns, so it cannot separate "checked and clean" from
   * "checked nothing" — the shape this project records as worse than a failure.
   * The count is compared against the records on disk rather than against a
   * literal, so adding a record does not edit this test.
   */
  @Test
  void everyCitationInThisRepositoryResolvesAndTheGateSaysHowMany() throws Exception {
    Run r = run(ROOT, "scripts/check-adr-refs.sh");
    assertThat(r.exit()).as("check-adr-refs.sh on this tree:%n%s", r.out()).isZero();

    long records;
    try (var files = Files.list(ROOT.resolve(DECISIONS))) {
      records = files.filter(f -> f.getFileName().toString().endsWith(".md")).count();
    }
    assertThat(r.out())
        .as("the gate must report the work it did -- a counter stuck at 0 prints a clean "
            + "line over a tree it never read%n%s", r.out())
        .contains(records + " citation(s) all resolve, against " + records + " records");
  }

  /**
   * ⚠️ THE DANGLING CITATION IS IN A {@code .java} FILE, and that is the point
   * rather than an arbitrary choice. Most citations in this tree live in
   * javadoc, and two of the restored records are cited from Java and nowhere
   * else — so a fixture that only ever dangles in markdown leaves "the gate
   * reads Java" unconstrained. Review MEASURED it: narrowing the gate to
   * markdown alone kept the whole suite green.
   */
  @Test
  void aCitationWithNoRecordIsREFUSED_AndTheGateNamesTheFile() throws Exception {
    Path repo = fixture("dangling-in-java");
    String dangling = PREFIX + "9998";
    Files.writeString(repo.resolve("src/Foo.java"),
        "/** Justified by " + dangling + ". */\nclass Foo { }\n");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-adr-refs.sh");

    assertThat(r.exit()).as("a dangling citation must FAIL:%n%s", r.out()).isEqualTo(1);
    assertThat(r.out())
        .as("it must name the one that dangles, not merely say something is wrong")
        .contains(dangling);
    // ⚠️ AND WHERE. A number alone sends the reader grepping for what the gate
    // has already found; deleting the whole `cited in` loop survives an
    // assertion that only checks the number, because the failure line names it
    // too.
    assertThat(r.out())
        .as("and the file carrying it")
        .contains("src/Foo.java");
    assertThat(r.out())
        .as("without accusing the citation that resolves")
        .doesNotContain(PREFIX + "0001 cited in");
  }

  /**
   * ⚠️ THE CITING FILE IS COMMITTED AND UNTOUCHED, and the record is deleted in
   * the staged diff. This is the whole case the gate's "always full scope"
   * refuses to give up, and it is invisible to a fixture that stages everything
   * without committing: there, tracked and staged are the same set, so a delta
   * gate and a full one cannot be told apart. Review MEASURED that — swapping
   * {@code workspace_files} for {@code scoped_files} kept the suite green.
   */
  @Test
  void aRecordDELETEDUnderAnUntouchedCitationIsREFUSED() throws Exception {
    Path repo = fixture("deleted-record");
    run(repo, "git", "add", "-A");
    run(repo, "git", "commit", "-qm", "fixture");

    // ⚠️ ONLY THE DELETION IS STAGED. `notes.md` is not in this diff, so a gate
    // judging the change rather than the tree sees nothing to read at all.
    run(repo, "git", "rm", "-q", DECISIONS + "/0001-a-real-one.md");

    Run r = run(repo, "scripts/check-adr-refs.sh");

    assertThat(r.exit())
        .as("deleting a record that an untouched file cites must FAIL:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains(PREFIX + "0001");
    assertThat(r.out())
        .as("naming the committed file the gate had to look outside the diff to find")
        .contains("notes.md");
  }

  /**
   * ⚠️ CITATIONS COME FROM GIT, SO RECORDS MUST TOO. An untracked file on disk
   * satisfies nobody else's clone, and pre-commit stashes unstaged changes to
   * TRACKED files only — so a record sitting untracked is present while the
   * hook runs and absent from the commit. Review MEASURED the first draft going
   * green exactly there, which is the defect this task exists to close, one
   * notch smaller: never staged instead of never merged.
   */
  @Test
  void aRecordThatIsUNTRACKEDDoesNotSatisfyACitation() throws Exception {
    Path repo = fixture("untracked-record");
    String orphan = PREFIX + "9997";
    Files.writeString(repo.resolve("src/Bar.java"),
        "/** Justified by " + orphan + ". */\nclass Bar { }\n");
    run(repo, "git", "add", "-A");
    // ⚠️ WRITTEN AFTER THE `git add`, so it is on disk and in no index.
    Files.writeString(repo.resolve(DECISIONS + "/9997-never-staged.md"),
        "# 9997. Never staged\n");

    Run r = run(repo, "scripts/check-adr-refs.sh");

    assertThat(r.exit())
        .as("a record no clone would receive must not satisfy a citation:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains(orphan);
  }
}
