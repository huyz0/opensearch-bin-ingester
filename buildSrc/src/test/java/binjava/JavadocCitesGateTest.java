// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Every test method a {@code src/main} javadoc names is a test method that
 * exists (M5.56).
 *
 * <p>⚠️ NAMING BEATS COUNTING <b>BECAUSE A NAME IS CHECKABLE</b>, which is
 * M5.34's own argument for citing a fixture rather than tallying one — and
 * three of that row's four names were split across {@code &#123;@code&#125;}
 * spans and grepped nowhere until review caught them by hand. A citation that
 * resolves for no reader is worse than the count it replaced: it reads as
 * evidence and is not.
 *
 * <p>⚠️ NON-NEGOTIABLE 9 PUTS THIS AT RUNG 3. It is a predicate over files, so
 * an agent must not be asked to check it, and a review round that catches one
 * instance leaves every other instance standing.
 *
 * <p>⚠️ THE SPLIT IS WHAT MAKES IT SILENT, and it is the shape both instances
 * that motivated this had: a name broken INSIDE the identifier leaves the first
 * half still parseable as a citation and dangling, while the full name greps
 * nowhere. That is why this gate resolves per LINE and never joins a javadoc's
 * lines back together — joining them would reconstruct a name no reader's
 * {@code grep} can find, and report the tree clean.
 *
 * <p>⚠️ A BREAK AT THE DOT IS A DIFFERENT SHAPE AND WAS SILENT UNTIL REVIEW
 * MEASURED IT: neither half matches a {@code Class.method} pattern, so the
 * per-line rule alone sees nothing. It is refused by a separate rule — a
 * {@code *Test}/{@code *IT} name inside an inline tag span left OPEN at end of
 * line — and each of the four refused shapes has a case below rather than a
 * sentence in the script.
 */
class JavadocCitesGateTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final Pattern CLEAN =
      Pattern.compile("(\\d+) citation\\(s\\) all resolve");

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
   * (testing.md rule 17), and one git repository per case rather than one
   * shared: these cases COMMIT and then stage, so a shared fixture would leak
   * one case's index into the next.
   */
  private static Path scratch(String name) throws Exception {
    Path dir = ROOT.resolve("buildSrc/build/tmp/javadoc-cites")
        .resolve(name + "-" + UUID.randomUUID());
    Files.createDirectories(dir);
    return dir;
  }

  /**
   * A repository with one production file, one test class, and a citation that
   * resolves.
   *
   * <p>⚠️ THE TEST CLASS CARRIES A NON-TEST HELPER TOO, so "resolves to a
   * method" and "resolves to a {@code @Test}-annotated method" are separable by
   * a case rather than by reading the script.
   */
  private static Path fixture(String name) throws Exception {
    Path repo = scratch(name);
    Files.createDirectories(repo.resolve("scripts"));
    Files.createDirectories(repo.resolve("mod/src/main/java/binjava/mod"));
    Files.createDirectories(repo.resolve("mod/src/test/java/binjava/mod"));
    for (String s : new String[] {"lib.sh", "check-javadoc-cites.sh"}) {
      Files.copy(ROOT.resolve("scripts/" + s), repo.resolve("scripts/" + s));
      repo.resolve("scripts/" + s).toFile().setExecutable(true);
    }
    for (String s : new String[] {"java_tests.py", "javadoc_cites.py"}) {
      Files.copy(ROOT.resolve("scripts/" + s), repo.resolve("scripts/" + s));
    }
    Files.writeString(repo.resolve("mod/src/test/java/binjava/mod/FooTest.java"),
        """
        package binjava.mod;
        import org.junit.jupiter.api.Test;
        class FooTest {
          private void aHelperThatIsNotATest() { }
          @Test void aRealCase() { }
          @Nested class NestedCasesTest {
            @Test void aNestedCase() { }
          }
        }
        """);
    // ⚠️ A SECOND SUFFIX IN THE FIXTURE, because `CITE`'s `IT` alternative was
    // reachable by nothing: review MEASURED `(?:Test|IT)` narrowed to
    // `(?:Test)` surviving every case AND leaving the real-tree count at 19,
    // so the floor is structurally blind to it -- every rule-1 citation in the
    // tree names a `*Test`, and its one `*IT` citation is a bare class going
    // through `NAME` instead. That is LIVE rather than latent: M5.76 records a
    // cluster-`*IT` method citation being refused today, which is behaviour
    // only this alternative produces.
    Files.writeString(repo.resolve("mod/src/test/java/binjava/mod/FooIT.java"),
        """
        package binjava.mod;
        import org.junit.jupiter.api.Test;
        class FooIT {
          @Test void aRealCase() { }
        }
        """);
    Files.writeString(repo.resolve("mod/src/main/java/binjava/mod/Prod.java"),
        """
        package binjava.mod;
        /** Pinned by {@code FooTest.aRealCase}. */
        class Prod { }
        """);
    run(repo, "git", "init", "-q");
    run(repo, "git", "config", "user.email", "t@example.com");
    run(repo, "git", "config", "user.name", "t");
    return repo;
  }

  private static void citeFrom(Path repo, String javadoc) throws Exception {
    Files.writeString(repo.resolve("mod/src/main/java/binjava/mod/Prod.java"),
        "package binjava.mod;\n/**\n" + javadoc + "\n */\nclass Prod { }\n");
  }

  /**
   * ⚠️ THE REAL TREE, not a fixture. A gate proved correct against scratch
   * repositories and never run against the corpus it was written for has never
   * met the citations that motivated it.
   *
   * <p>⚠️ AND THE COUNT IS ASSERTED TO BE NON-ZERO rather than transcribed. Exit
   * 0 is what a gate that read nothing returns, so the status alone cannot
   * separate "checked and clean" from "checked nothing" — and a transcribed
   * literal is a fact with no source that goes stale the next time anyone cites
   * a fixture.
   */
  @Test
  void everyCitationInThisRepositoryResolvesAndTheGateSaysHowMany() throws Exception {
    Run r = run(ROOT, "scripts/check-javadoc-cites.sh");
    assertThat(r.exit()).as("check-javadoc-cites.sh on this tree:%n%s", r.out()).isZero();

    Matcher m = CLEAN.matcher(r.out());
    assertThat(m.find()).as("the gate must report the work it did:%n%s", r.out()).isTrue();
    // ⚠️ A FLOOR, NOT A LITERAL, and the difference is what makes it worth
    // having: `isPositive` alone left the gate's REACH unconstrained, and
    // review measured two narrowings surviving it — dropping the `#` spelling
    // and reading no file under `ingest/` or `sequencer/` both kept six of six
    // green while the tree's count fell. A floor only ever moves UPWARD, so it
    // is not the transcribed count this file's own javadoc rejects elsewhere.
    // ⚠️ RE-RUN IT RATHER THAN TRUSTING THIS NUMBER:
    //   scripts/check-javadoc-cites.sh
    // Lowering it is a deliberate act that must say which citation went away.
    assertThat(Integer.parseInt(m.group(1)))
        .as("a counter stuck at 0 prints a clean line over a tree it never read, "
            + "and a shrinking one means the gate stopped reading part of it%n%s", r.out())
        .isGreaterThanOrEqualTo(19);
    assertThat(r.out())
        .as("and it must say which scope it ran at -- a delta pass is a weaker "
            + "statement than a full one and must not read like one")
        .contains("scope: full");
    // ⚠️ THE INDEX COUNTS ARE REPORTAGE AND WERE HARDCODEABLE TO ZERO, measured
    // surviving every case: the gate then reads `against 0 test method(s) in 0
    // class(es)` while resolving 19 citations, which is a line that cannot be
    // true and that nobody would question.
    assertThat(r.out())
        .as("a gate resolving citations against nothing is reporting a lie%n%s", r.out())
        .doesNotContain("against 0 test method(s) in 0 class(es)");
  }

  /**
   * ⚠️ {@code open_tail}'s BOOKKEEPING, WHICH IS THE WHOLE OF RULE 3 and which
   * every other fixture leaves untouched because their lines carry exactly one
   * brace token. Review measured both of its branches mutable, one in each
   * direction, on lines this shape.
   *
   * <p>⚠️ THE CLOSE-THEN-OPEN LINE IS THE MISSED-DEFECT DIRECTION: dropping the
   * {@code max(0, ...)} floor lets the depth go negative on the leading
   * {@code &#125;}, so the span opened afterwards is never seen and the split is
   * silently missed. Six lines of exactly this shape are live in
   * {@code src/main} today; none carries a test name yet, which is the only
   * reason the real-tree case stays green over the mutant.
   */
  @Test
  void aSpanOpenedAFTERAnotherCLOSESOnTheSameLineIsStillOpen() throws Exception {
    Path repo = fixture("close-then-open");
    // ⚠️ THE LINE MUST BEGIN WITH THE CLOSING BRACE, which is the whole of why
    // the mutation bites: only then does the depth counter go NEGATIVE without
    // the floor, and only a negative depth makes the `depth == 0` test for the
    // next opener false. A first draft of this fixture opened and closed a span
    // on the same line before opening the second, never went below zero, and
    // review would have measured the mutation surviving it.
    citeFrom(repo, " * Pinned by {@code some\n"
        + " * prose} and then {@code FooTest.aReal\n"
        + " * Case}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("the second span is open at end of line and carries a test name:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("SPLIT across lines");
  }

  /**
   * ⚠️ THE FALSE-REFUSAL DIRECTION OF THE SAME BOOKKEEPING. A stray
   * {@code &#123;} after a span has closed must not reopen it: without the
   * {@code start = None} reset the depth climbs again, the resolving citation
   * on that line is re-read as a split, and a correct javadoc is refused with
   * no remedy but {@code SKIP=}.
   */
  @Test
  void aSTRAYBraceAfterAClosedSpanDoesNotReopenIt() throws Exception {
    Path repo = fixture("stray-brace");
    citeFrom(repo, " * Pinned by {@code FooTest.aRealCase}, and a stray brace { here.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a resolving citation must not be refused because a brace follows it:%n%s",
            r.out())
        .isZero();
  }

  /**
   * ⚠️ {@code SubscriptionHub}'s LIVE SHAPE, reproduced rather than described:
   * one span left OPEN at a line end with the name broken mid-identifier, so
   * the first line still parses as a citation naming a method that does not
   * exist. A gate that joined the javadoc's lines would resolve it and report
   * the tree clean, which is exactly the silence this task exists to end.
   *
   * <p>⚠️ IT IS NOT {@code Sequencer}'s SHAPE, and two drafts of this comment
   * claimed both live instances at once. Review measured the difference: this
   * fixture fires rule 3 as well as rule 1 because the span stays open, and
   * {@code Sequencer}'s two CLOSED spans fire rule 1 alone. That case is
   * {@link #aCitationSPLITAcrossTWOCLOSEDSpansIsREFUSED_ByRuleOneAlone}.
   */
  @Test
  void aCitationSPLITAcrossALineBreakIsREFUSED_AndTheGateNamesFileAndLine()
      throws Exception {
    Path repo = fixture("split");
    citeFrom(repo, " * Pinned by {@code FooTest.aReal\n * Case}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit()).as("a split citation must FAIL:%n%s", r.out()).isEqualTo(1);
    assertThat(r.out())
        .as("naming the dangling half, not merely saying something is wrong")
        .contains("FooTest.aReal");
    // ⚠️ AND WHERE. A name alone sends the reader grepping for what the gate has
    // already located, and the line number is the only part that distinguishes
    // the two halves of a split.
    assertThat(r.out())
        .as("and the file and line carrying it")
        .contains("mod/src/main/java/binjava/mod/Prod.java:3");
  }

  /**
   * ⚠️ A METHOD THAT EXISTS BUT IS NOT A TEST DOES NOT SATISFY A CITATION, and
   * this is the half a simple "does this identifier appear in the test file"
   * gate cannot see. A citation exists to name a fixture a reader can run.
   */
  @Test
  void aCitationNamingANonTestMethodOfATestClassIsREFUSED() throws Exception {
    Path repo = fixture("not-a-test");
    citeFrom(repo, " * Pinned by {@code FooTest.aHelperThatIsNotATest}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a helper is not a fixture a reader can run:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("FooTest.aHelperThatIsNotATest");
  }

  /**
   * ⚠️ THE CITING FILE IS COMMITTED AND UNTOUCHED, and only the RENAME is
   * staged. This is the whole case the row's "it MUST run at full scope"
   * requires: renaming a test touches no {@code src/main} file, so the change
   * that breaks the citation never puts the broken file in a delta. A gate
   * judging the diff sees nothing to read at all and prints a clean line.
   */
  @Test
  void aTestRENAMEDUnderAnUntouchedCitationIsREFUSED() throws Exception {
    Path repo = fixture("renamed-test");
    run(repo, "git", "add", "-A");
    run(repo, "git", "commit", "-qm", "fixture");

    Files.writeString(repo.resolve("mod/src/test/java/binjava/mod/FooTest.java"),
        """
        package binjava.mod;
        import org.junit.jupiter.api.Test;
        class FooTest {
          private void aHelperThatIsNotATest() { }
          @Test void aRealCaseUnderItsNEWName() { }
        }
        """);
    run(repo, "git", "add", "mod/src/test/java/binjava/mod/FooTest.java");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("renaming a cited test must FAIL even though no src/main file "
            + "is in the diff:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("FooTest.aRealCase");
    assertThat(r.out())
        .as("naming the committed file the gate had to look outside the diff to find")
        .contains("Prod.java");
  }

  /**
   * ⚠️ THE FALSE-POSITIVE SURFACE IS WHAT KILLS A GATE LIKE THIS, so both
   * exclusions are pinned here rather than left to the script's comments: a
   * class name mentioned with no method is prose, and {@code Foo.bar} on a
   * class that is not a test class is every dotted phrase in every javadoc in
   * the tree.
   */
  @Test
  void aBARECLASSNameAndANonTestClassAreNotCitations() throws Exception {
    Path repo = fixture("prose");
    citeFrom(repo, " * See FooTest, and note that Duration.ofSeconds is not a\n"
        + " * citation, nor is binjava.mod.Prod.toString.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("prose must not be read as a citation:%n%s", r.out())
        .isZero();
  }

  /**
   * ⚠️ A CITATION IS RESOLVED AGAINST GIT, NOT THE FILESYSTEM. A test file
   * sitting untracked satisfies no other clone, and pre-commit stashes unstaged
   * changes to TRACKED files only — so it is present while the hook runs and
   * absent from the commit.
   */
  @Test
  void aTestFileThatIsUNTRACKEDDoesNotSatisfyACitation() throws Exception {
    Path repo = fixture("untracked-test");
    citeFrom(repo, " * Pinned by {@code BarTest.aCaseInAnUntrackedFile}.");
    run(repo, "git", "add", "-A");
    // ⚠️ WRITTEN AFTER THE `git add`, so it is on disk and in no index.
    Files.writeString(repo.resolve("mod/src/test/java/binjava/mod/BarTest.java"),
        """
        package binjava.mod;
        import org.junit.jupiter.api.Test;
        class BarTest {
          @Test void aCaseInAnUntrackedFile() { }
        }
        """);

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a test no clone would receive must not satisfy a citation:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("BarTest.aCaseInAnUntrackedFile");
  }

  /**
   * ⚠️ THE OTHER SPLIT SHAPE, which the per-line rule alone cannot see: the
   * break lands on the DOT, so neither line carries a {@code Class.method}
   * pattern and both halves are invisible. Review MEASURED the round-1 gate
   * printing {@code __SUMMARY__ 0 0 1 1} and exiting 0 over exactly this. The
   * rule that refuses it is about the SPAN rather than the name — an inline tag
   * left open at end of line — which is why it catches the break wherever it
   * lands.
   */
  @ParameterizedTest
  @ValueSource(strings = {"code", "link"})
  void aCitationSPLITAtTheDOTIsREFUSED(String tag) throws Exception {
    Path repo = fixture("split-at-dot-" + tag);
    citeFrom(repo, " * Pinned by {@" + tag + " FooTest.\n * aRealCase}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit()).as("a citation split at its dot must FAIL:%n%s", r.out()).isEqualTo(1);
    assertThat(r.out()).contains("FooTest");
    assertThat(r.out())
        .as("and it must say the citation is split rather than that a method is missing")
        .contains("SPLIT across lines");
    // ⚠️ THE COUNT AND THE FINDINGS COME FROM ONE TRAVERSAL, and nothing
    // asserted it: review MEASURED deleting either failing branch's `cited +=
    // 1` surviving all sixteen executions, which restores the `1 of 0
    // citation(s)` line this round changed production to remove. The clean-tree
    // floor cannot see it, because no failing branch runs on a clean tree.
    assertThat(r.out())
        .as("a split citation is still A citation -- `1 of 0` reads as a bug in the gate")
        .contains("1 of 1 citation(s)");
    // ⚠️ AND THE ADVICE IS ASSERTED HERE AND DENIED BELOW. Both directions of
    // its gating survived: restoring `if bad:` and deleting the block outright.
    assertThat(r.out())
        .as("a split is exactly what the line-break advice is for")
        .contains("Put each citation on ONE line");
  }

  /**
   * ⚠️ THE FOURTH REFUSED SHAPE, WHICH THE SCRIPT'S OWN HEADER CALLS PART OF
   * "THE COVERAGE CLAIM" AND NOTHING ASSERTED. Review MEASURED two mutations
   * surviving all twelve cases: deleting the rule, and widening its guard by
   * one token so it stays alive for shapes that never occur while dying for
   * the canonical one.
   *
   * <p>⚠️ IT IS NOT THE OPEN-SPAN CASE ABOVE, and the difference is the whole
   * reason it was missed: there the break leaves the span UNCLOSED and
   * {@code open_tail} sees it, here both spans close on their own line and only
   * the trailing separator says a name was cut in half.
   */
  @ParameterizedTest
  @ValueSource(strings = {"code", "link"})
  void aCitationENDINGATItsSeparatorIsREFUSED(String tag) throws Exception {
    Path repo = fixture("ends-at-separator-" + tag);
    citeFrom(repo, " * Pinned by {@" + tag + " FooTest.}\n * {@" + tag + " aRealCase}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a citation cut off at its separator must FAIL:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out())
        .as("and say the name is cut short, not that a method is missing")
        .contains("ends at its separator");
    // ⚠️ THE SECOND FAILING BRANCH THAT INCREMENTS THE COUNT, and it needs its
    // own assertion: review measured `cited += 1` deletable HERE while the
    // split case above kept the other one honest.
    assertThat(r.out())
        .as("a cut-short citation is still A citation -- `1 of 0` reads as a gate bug")
        .contains("1 of 1 citation(s)");
    // ⚠️ THE OTHER HALF OF THE ADVICE GATING. Review measured the `separator`
    // arm of its condition deletable while the `SPLIT` arm stayed pinned.
    // ⚠️ THIS IS NOT `Sequencer`'s SHAPE EITHER, and an earlier draft said it
    // was: `git log -S` shows that file never held a break on the dot.
    assertThat(r.out())
        .as("a name cut at its separator is a line-break defect and gets the advice")
        .contains("Put each citation on ONE line");
  }

  /**
   * ⚠️ A BARE CLASS NAME INSIDE AN INLINE TAG IS A CITATION, and the round-1
   * gate read one only when a method followed it. Review found a LIVE dangling
   * instance the gate walked past — {@code BulkService}'s citation of an
   * {@code *IT} that has never existed under that name — while the script's own
   * header claimed the tree could hold no such thing.
   *
   * <p>⚠️ A CLASS RESOLVES AGAINST TEST SOURCE FILE NAMES, not against the
   * parsed index, and that is what keeps the rule from refusing every
   * {@code *IT} under {@code src/clusterTest}: those six classes extend
   * OpenSearch's test base classes and name their cases {@code void testX()} by
   * the JUnit3 convention, with no annotation at all, so the parsed index holds
   * no methods for them. ⚠️ AN EARLIER DRAFT CALLED THAT A COMPOSED ANNOTATION
   * AND BLAMED M0.17, a different blind spot that would not change these six.
   * It survived a round here because the phrase wrapped across a javadoc line
   * break and the sweep's grep could not see it.
   */
  @Test
  void aBARECLASSNameInsideACodeSpanMustNameATestFile() throws Exception {
    Path repo = fixture("bare-class");
    citeFrom(repo, " * Pinned by {@code NoSuchFixtureTest}, which does not exist.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a bare class citation naming no test file must FAIL:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("NoSuchFixtureTest");
    assertThat(r.out())
        .as("nothing here is split, so advice about line breaks is noise that sends "
            + "the reader looking for a break that does not exist")
        .doesNotContain("Put each citation on ONE line");
    // ⚠️ RULE 2's OWN COUNTING SITE. F7 was closed at the two sites the finding
    // NAMED, and review measured the identical `1 of 0` regression surviving at
    // the other two -- fixing what a finding lists rather than what it is about.
    assertThat(r.out())
        .as("a dangling bare class is still A citation")
        .contains("1 of 1 citation(s)");
  }

  /**
   * ⚠️ BOTH SUFFIXES, because {@code CITE}'s {@code IT} alternative was
   * constrained by nothing. Review MEASURED {@code (?:Test|IT)} narrowed to
   * {@code (?:Test)} surviving every case here AND leaving the real tree at its
   * usual count, so neither the cases nor the floor could see it: every rule-1
   * citation in this repository names a {@code *Test}, and its one {@code *IT}
   * citation is a BARE class, which goes through {@code NAME} instead.
   *
   * <p>⚠️ IT IS LIVE, NOT LATENT, which is why it is a case rather than a row:
   * M5.76 records that citing a real cluster-{@code *IT} METHOD is refused
   * today, and that refusal is behaviour only this alternative produces.
   */
  @ParameterizedTest
  @ValueSource(strings = {"FooTest", "FooIT"})
  void aCitationNamingAMethodThatDoesNotExistIsREFUSED_UnderEITHERSuffix(String cls)
      throws Exception {
    Path repo = fixture("suffix-" + cls);
    citeFrom(repo, " * Pinned by {@code " + cls + ".aCaseThatWasNeverWritten}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("%s's dangling method citation must FAIL:%n%s", cls, r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains(cls + ".aCaseThatWasNeverWritten");
  }

  /**
   * ⚠️ THE {@code #} SPELLING IS THE {@code @link} FORM AND IS THE SAME DEFECT,
   * and no case exercised it: review MEASURED narrowing the separator to
   * {@code [.]} alone keeping every case green while the real tree's citation
   * count fell and two live citations stopped being read.
   */
  @Test
  void aCitationInTheHASHSpellingIsResolvedToo() throws Exception {
    Path repo = fixture("hash-spelling");
    citeFrom(repo, " * Pinned by {@link FooTest#aCaseThatWasNeverWritten}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("the @link spelling must be resolved like the dotted one:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("FooTest#aCaseThatWasNeverWritten");
    // ⚠️ RULE 1's COUNTING SITE, the commonest failure shape of all and the
    // last of the four review measured the `1 of 0` regression surviving at.
    assertThat(r.out())
        .as("a dangling method citation is still A citation")
        .contains("1 of 1 citation(s)");
    assertThat(r.out())
        .as("and nothing here is split, so the line-break advice is noise")
        .doesNotContain("Put each citation on ONE line");
  }

  /**
   * ⚠️ A NESTED TEST CLASS IS CITED BY WHICHEVER NAME A READER WOULD TYPE, so
   * its methods are filed under the outer and the inner simple name alike. The
   * tree has no nested test class today, which made the dual filing an
   * equivalent mutant — a fixture is the only thing that can pin it before the
   * first one arrives.
   */
  @Test
  void aNESTEDTestClassIsCitableByItsOwnSimpleName() throws Exception {
    Path repo = fixture("nested");
    citeFrom(repo, " * Pinned by {@code NestedCasesTest.aNestedCase}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a nested class's own name must resolve:%n%s", r.out())
        .isZero();
  }

  /**
   * ⚠️ A PARSER THAT LOST ITS PLACE MUST REFUSE, NEVER SKIP, and this is the
   * one branch whose whole product is a failure. Turning it into a warn — the
   * most tempting edit the first time a legitimate file trips the brace walk —
   * leaves the gate printing a clean line while resolving citations against an
   * index that silently lost a whole class. Review MEASURED that mutation
   * surviving every other case here.
   *
   * <p>⚠️ THE UNPARSEABLE FILE IS ANOTHER GATE'S OWN FIXTURE, copied rather
   * than invented, so this case cannot drift from what {@code java_tests.py}
   * actually refuses.
   */
  @Test
  void anUNPARSEABLETestSourceIsREFUSED_NeverSkipped() throws Exception {
    Path repo = fixture("unparseable");
    Files.copy(ROOT.resolve("buildSrc/src/test/resources/java-fixtures/refused/"
            + "count-mismatch.java"),
        repo.resolve("mod/src/test/java/binjava/mod/CountMismatchTest.java"));
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a test source the parser cannot read must FAIL, not be skipped:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out())
        .as("saying the file could not be read, rather than blaming a citation")
        .contains("could not be parsed");
  }

  /**
   * ⚠️ A PREDICATE THAT DID NOT RUN IS A FAILURE, NOT A PASS. With python3
   * exiting non-zero the captured output is empty, and a gate reading that as
   * "nothing dangling" prints the green line over nothing that
   * non-negotiable 4 is about — the shape {@code check-fault-store-records.sh}
   * records being hit once already.
   */
  @Test
  void theGateFAILSWhenThePredicateDidNotRunAtAll() throws Exception {
    Path repo = fixture("no-predicate");
    run(repo, "git", "add", "-A");
    // ⚠️ THE RESOLVER IS REMOVED AFTER THE FIXTURE BUILT IT, which is the
    // closest reachable stand-in for python3 itself being absent or broken.
    Files.delete(repo.resolve("scripts/javadoc_cites.py"));

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a gate whose predicate did not run must FAIL:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("did not run");
  }

  /**
   * ⚠️ THE FALSE-REFUSAL DIRECTION, which `gate-design` step 3 asks for and the
   * first draft of this gate had no case for. Review MEASURED three:
   * {@code LIMIT}, {@code MAX_UNIT} and {@code WAIT_LIMIT} were each reported
   * as citations naming no test source file, with no remedy but {@code SKIP=}.
   * An ALL-CAPS identifier is a constant, never a class name.
   */
  @Test
  void anALLCAPSConstantInACodeSpanIsNOTACitation() throws Exception {
    Path repo = fixture("all-caps");
    citeFrom(repo, " * Bounded by {@code LIMIT}, {@code MAX_UNIT} and {@code COMMIT}.");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("a constant is not a dangling citation:%n%s", r.out())
        .isZero();
  }


  /**
   * ⚠️ {@code Sequencer}'s LIVE SHAPE, WHICH NO CASE REACHED FOR FIVE ROUNDS
   * while two comments each claimed to be it. Two CLOSED spans with the break
   * inside the METHOD identifier: {@code SPAN} matches both, {@code open_tail}
   * returns nothing because neither line leaves a span open, and the
   * continuation line carries no {@code *Test} token at all. So rule 1 alone
   * fires, on the truncated method name.
   *
   * <p>⚠️ AND THE MESSAGE IT GETS IS THE WRONG ONE, asserted here so the
   * limitation is visible rather than discovered again: rule 1's reason is
   * "names no directly-@Test-annotated method", and the line-break advice is
   * withheld because the advice condition greps {@code SPLIT}/{@code separator}
   * out of the reason strings. An author reading it goes looking for a missing
   * test rather than rejoining a line. M5.74 owns the message; this case owns
   * the refusal, which is what must not regress.
   */
  @Test
  void aCitationSPLITAcrossTWOCLOSEDSpansIsREFUSED_ByRuleOneAlone() throws Exception {
    Path repo = fixture("two-closed-spans");
    citeFrom(repo, " * Pinned by {@code FooTest.aRealCaseWithALong}\n"
        + " * {@code NameThatContinues} (M5.52b).");
    run(repo, "git", "add", "-A");

    Run r = run(repo, "scripts/check-javadoc-cites.sh");

    assertThat(r.exit())
        .as("the shape this task exists to catch must FAIL:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out())
        .as("naming the TRUNCATED method, which is what greps nowhere")
        .contains("FooTest.aRealCaseWithALong");
    assertThat(r.out())
        .as("and it is one citation, not zero")
        .contains("1 of 1 citation(s)");
    // ⚠️ THE LIMITATION, ASSERTED RATHER THAN DESCRIBED. Review measured the
    // javadoc above CLAIMING this was asserted when nothing here touched the
    // message -- the same defect class this task spent six rounds on. It is a
    // real assertion now, and it is deliberately the one that will RED when
    // M5.74 lands: whoever fixes the message must come here and say so.
    assertThat(r.out())
        .as("today this shape is refused through rule 1 alone, so the line-break "
            + "advice is withheld and the message sends the reader hunting a missing "
            + "test -- M5.74 owns changing that")
        .doesNotContain("Put each citation on ONE line");
  }
}
