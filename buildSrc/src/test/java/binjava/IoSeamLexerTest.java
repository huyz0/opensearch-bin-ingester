// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The lexer half of non-negotiable 7's gate: what counts as CODE.
 *
 * <p>⚠️ A SEPARATE FILE UNDER code-structure.md rule 1, no line-count trigger
 * claimed (M5.54). The seam is real rather
 * than arithmetic: every case here is about {@code io_seam_scan.code_only}
 * deciding which bytes are code, and every case left behind is about what the
 * gate then DOES with them -- its scope, its one exemption, its reporting and
 * its error paths.
 *
 * <p>⚠️ THIS HALF IS WHERE THE DEFECTS WERE. Three review rounds walked past
 * the scanner and every route was a lexing mistake: a {@code //} inside a
 * string, a quote inside a char literal, an escaped delimiter inside a text
 * block, and a comment that never ended. A hand-written lexer gets each
 * construct right one at a time, and each one needs a case -- which is the
 * argument for keeping them together under one name.
 */
class IoSeamLexerTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final String GATE = "scripts/check-io-seam.sh";
  private static final String SCANNER = "scripts/io_seam_scan.py";
  private static final String MAIN = "ingest/src/main/java/binjava/ingest";

  private record Run(int exit, String out) {
  }

  private static Run run(Path dir, String... command) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(command).directory(dir.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());
    return new Run(p.waitFor(), out);
  }

  /** ⚠️ SCRATCH UNDER {@code build/tmp}, never the system temp (testing.md 17). */
  private static Path fixture(String name) throws Exception {
    Path repo = ROOT.resolve("buildSrc/build/tmp/io-seam")
        .resolve(name + "-" + UUID.randomUUID());
    Files.createDirectories(repo.resolve("scripts"));
    Files.createDirectories(repo.resolve(MAIN));
    Files.copy(ROOT.resolve("scripts/lib.sh"), repo.resolve("scripts/lib.sh"));
    Files.copy(ROOT.resolve(GATE), repo.resolve(GATE));
    Files.copy(ROOT.resolve(SCANNER), repo.resolve(SCANNER));
    repo.resolve(GATE).toFile().setExecutable(true);
    run(repo, "git", "init", "-q");
    run(repo, "git", "config", "user.email", "t@example.com");
    run(repo, "git", "config", "user.name", "t");
    run(repo, "git", "add", "-A");
    return repo;
  }

  private static Run onFixture(String name, String path, String body) throws Exception {
    Path repo = fixture(name);
    Files.createDirectories(repo.resolve(path).getParent());
    Files.writeString(repo.resolve(path), body);
    run(repo, "git", "add", "-A");
    return run(repo, GATE);
  }

  /**
   * A banned call QUOTED in a comment or a literal is prose, not a violation —
   * in a line comment, a MULTI-LINE javadoc, a string and a TEXT BLOCK.
   *
   * <p>⚠️ ALL FOUR, BECAUSE THREE OF THEM WERE UNPINNED AND REVIEW MEASURED IT.
   * A single-line fixture left the multi-line case free: dropping
   * {@code re.DOTALL} from the old block-comment pattern survived every case
   * AND the real tree, while redding any multi-line javadoc that quotes a
   * banned call — which is the shape of most javadoc in this repository. The
   * text block was not handled at all and was reported as a violation.
   *
   * <p>⚠️ THIS TREE MAKES IT LOAD-BEARING RATHER THAN THEORETICAL: its comments
   * quote code constantly, and the backlog row that opened this gate quotes
   * {@code Files.readAllBytes}. A gate that reds on a javadoc saying "never
   * call this" gets weakened rather than obeyed — the direction non-negotiable
   * 2 forbids.
   */
  @Test
  void aBannedCallQUOTEDInACommentOrALiteralIsNotAViolation() throws Exception {
    Run r = onFixture("quoted", MAIN + "/Quoting.java",
        "package binjava.ingest;\n"
            + "/**\n"
            + " * Never source the clock here -- no Instant.now(), and never\n"
            + " * Files.readAllBytes either, across two lines of javadoc.\n"
            + " */\n"
            + "class Quoting {\n"
            + "  // and not new Socket( either\n"
            + "  String why() { return \"forbidden: Instant.now() and Files.\"; }\n"
            + "  String block() { return \"\"\"\n"
            + "      a text block quoting Instant.now() and Files.readAllBytes\n"
            + "      \"\"\"; }\n"
            + "}\n");

    assertThat(r.exit())
        .as("quoting a banned call is prose, not a call:%n%s", r.out())
        .isZero();
  }

  /**
   * ⚠️ ONE CHAR LITERAL HOLDING A QUOTE USED TO BLIND THE WHOLE SCANNER, and
   * review MEASURED the live cost: this exact file was reported CLEAN while
   * reading {@code /etc/hostname}. The old stripper ran a sequence of regexes
   * and the char literal mis-paired the string pattern, which then blanked
   * forward to the next quote and swallowed the filesystem call. Its {@code
   * CHAR} pattern ran AFTER {@code STRING}, so it was dead code by
   * construction — deleting it changed nothing.
   *
   * <p>⚠️ THE ANSWER WAS A SINGLE LEFT-TO-RIGHT PASS, not another regex —
   * a scanner that never looks at a delimiter already inside something else
   * removed both of round 1's routes at once, which is why the fix was a
   * rewrite rather than a reorder. ⚠️ IT IS STILL A LEXER AND NOT A PARSER,
   * and "that class of bug is impossible now" is the claim an earlier draft of
   * this javadoc made and the NEXT round falsified — see
   * {@link #anESCAPEDDelimiterInsideATextBlockDoesNotBlankTheRestOfTheFile}.
   */
  @Test
  void aCHARLiteralHoldingAQuoteDoesNotBlindTheScanner() throws Exception {
    Run r = onFixture("char-blind", MAIN + "/Blinder.java",
        "package binjava.ingest;\nclass Blinder { String f(char c) throws Exception "
            + "{ return c == '\"' ? \"yes\" "
            + ": java.nio.file.Files.readString(java.nio.file.Path.of(\"/etc/hostname\")); } }\n");

    assertThat(r.exit())
        .as("a filesystem read behind a char literal is still a filesystem read:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("java.nio.file");
  }

  /**
   * ⚠️ ONE ESCAPED DELIMITER INSIDE A TEXT BLOCK USED TO BLANK EVERYTHING AFTER
   * IT. Review MEASURED the live cost against the rewritten scanner: a file
   * carrying {@code \\"""} inside a text block and a
   * {@code Files.readString(Path.of("/etc/hostname"))} in a LATER method was
   * reported clean. The search for the closing delimiter landed on the escaped
   * one, the real closing delimiter was then read as a new OPENER, and the rest
   * of the file went with it.
   *
   * <p>⚠️ IT IS THE SAME DEFECT AS THE CHAR LITERAL ONE ROUND EARLIER, in the
   * one branch that had not been given escape handling — which is why the
   * header of this suite no longer claims the rewrite made the class of bug
   * impossible. A hand-written lexer gets each construct right one at a time,
   * and each one needs a case.
   */
  @Test
  void anESCAPEDDelimiterInsideATextBlockDoesNotBlankTheRestOfTheFile() throws Exception {
    Run r = onFixture("textblock-escape", MAIN + "/Escaped.java",
        "package binjava.ingest;\nclass Escaped {\n"
            + "  String quoted() { return \"\"\"\n"
            + "      pass \\\"\"\" to quote a text block\n"
            + "      \"\"\"; }\n"
            + "  String read() throws Exception { return java.nio.file.Files.readString("
            + "java.nio.file.Path.of(\"/etc/hostname\")); }\n}\n");

    assertThat(r.exit())
        .as("a read AFTER an escaped delimiter is still a read:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("java.nio.file");
  }

  /**
   * ⚠️ AN ESCAPED QUOTE INSIDE A STRING IS NOT THE END OF THE STRING, and the
   * branch that knows so was unpinned. Review MEASURED the mutation -- dropping
   * the escape step from the string and char branch -- surviving every case,
   * and failing in BOTH directions: prose after an escaped quote is falsely
   * REFUSED, and a real read after one is silently MISSED. Three tracked
   * {@code src/main} files already carry an escaped quote.
   *
   * <p>⚠️ IT IS THE SAME DEFECT AS THE TEXT-BLOCK ONE, in the sibling branch:
   * that one got escape handling AND a case in the same round, this one got the
   * handling and no case. A hand-written lexer needs a case per construct.
   */
  @Test
  void anESCAPEDQuoteInsideAStringDoesNotEndIt() throws Exception {
    Run missed = onFixture("escape-missed", MAIN + "/Escape.java",
        "package binjava.ingest;\nclass Escape { Object read(java.nio.file.Path p) "
            + "throws Exception { String q = \"\\\"\"; "
            + "return q + java.nio.file.Files.readString(p); } }\n");
    assertThat(missed.exit())
        .as("a read after an escaped quote is still a read:%n%s", missed.out())
        .isEqualTo(1);

    Run prose = onFixture("escape-prose", MAIN + "/Prose.java",
        "package binjava.ingest;\nclass Prose { String why() "
            + "{ return \"quote: \\\" and the words java.nio.file.Files in prose\"; } }\n");
    assertThat(prose.exit())
        .as("and prose after an escaped quote is still prose:%n%s", prose.out())
        .isZero();
  }

  /**
   * ⚠️ THE SCANNER RESUMES AFTER A COMMENT ENDS, and nothing pinned that until
   * review MEASURED the cost twice over: in {@code code_only}, making the line
   * comment branch run to end of file, or the block comment branch do the same,
   * survived every case -- while a class with a comment on one line and a
   * filesystem read on the next reported CLEAN. Nearly every file in this tree
   * opens with a javadoc, so either mutation blinds the gate TREE-WIDE while
   * the success line still prints the right file count.
   *
   * <p>⚠️ THE QUOTING CASE CANNOT SEE IT, which is why this is separate: there
   * every banned construct sits INSIDE a comment and none is a real call, so it
   * pins the false-positive direction only. Here the call is after the comment.
   */
  @Test
  void aRealCallAFTERACommentIsSTILLCounted() throws Exception {
    Run line = onFixture("resume-line", MAIN + "/AfterLine.java",
        "package binjava.ingest;\nclass AfterLine {\n"
            + "  // an ordinary comment\n"
            + "  Object read(java.nio.file.Path p) throws Exception "
            + "{ return java.nio.file.Files.readAllBytes(p); } }\n");
    assertThat(line.exit())
        .as("a call after a LINE comment is still a call:%n%s", line.out())
        .isEqualTo(1);

    Run block = onFixture("resume-block", MAIN + "/AfterBlock.java",
        "package binjava.ingest;\n/** Javadoc first,\n * across two lines. */\n"
            + "class AfterBlock {\n"
            + "  long when() { return nanoTime(); } }\n");
    assertThat(block.exit())
        .as("a call after a BLOCK comment is still a call:%n%s", block.out())
        .isEqualTo(1);
  }

  /** The violation names the LINE, not just the file. */
  @Test
  void theViolationNamesTheFileANDTheLINE() throws Exception {
    Run r = onFixture("line", MAIN + "/Dirty.java",
        "package binjava.ingest;\n/** A javadoc\n * spanning\n * four\n * lines. */\n"
            + "class Dirty {\n"
            + "  long when() { return nanoTime(); }\n}\n");

    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.out())
        .as("a gate that cannot point at the line makes the reader search -- and the "
            + "multi-line javadoc is the point: blanking a comment must PRESERVE its "
            + "newlines, or every line number after one is wrong%n%s", r.out())
        .contains("Dirty.java:7:");
  }
}
