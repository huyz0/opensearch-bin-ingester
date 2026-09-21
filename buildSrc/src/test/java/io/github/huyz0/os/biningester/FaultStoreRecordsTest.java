// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * `check-fault-store-records.sh` (M5.49, non-negotiable 9).
 *
 * <p>⚠️ THE GATE IS TESTED AGAINST FIXTURES, NOT AGAINST THE REAL STORE. Pointing
 * it only at `FaultInjectingStore.java` would test it solely by the thing it
 * guards: the gate would pass, and the only way to see it FAIL would be to break
 * the real store. A gate nobody has watched refuse is a gate nobody knows works.
 */
class FaultStoreRecordsTest {

  private static Path repo() {
    return Path.of("..").toAbsolutePath().normalize();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "metered                    | 0 | verb(s) metered",
        // ⚠️ THE UNMETERED VERB IS FIRST IN ITS FIXTURE, not last. Review
        // measured the old line-count window reaching PAST a short method into
        // the next one's record(...) -- five of the real store's twelve verbs
        // passed unmetered under it -- and the fixture could not see it because
        // nothing followed the unmetered verb to lend it a call.
        "unmetered                  | 1 | not metered",
        "far-away                   | 1 | not metered",
        // ⚠️ record(...) present, inside the body, reachable -- but an early
        // return above it meters ONE branch. "First statement" is claimed in
        // the failure message and the hook name; this is what makes it true.
        "metered-on-one-branch-only | 1 | first statement is",
        // ⚠️ A substring in a comment is not a call.
        "commented-out              | 1 | first statement is",
        // ⚠️ Zero verbs is a FAILURE, not `ok 0 verb(s) metered`. The store is
        // at exactly 700 lines, so its next change is a split; a split that
        // moved the verbs would leave this gate green over nothing forever.
        "no-verbs                   | 1 | lost its subject",
        // ⚠️ THE UNMETERED VERB IS IN THE MIDDLE, and first and last are both
        // blind spots in opposite directions. The original fixture put it LAST,
        // where nothing follows to lend it a record( — invisible to a
        // forward-bleeding window. Round 1 moved it FIRST to kill that, and
        // review measured what the move left open: "examine only the first
        // @Override" survived every fixture, because every fixture's unmetered
        // verb was the one a truncated scan still reaches.
        "unmetered-in-the-middle    | 1 | presign",
        // ⚠️ THE CALL IS ON THE BODY'S FIRST LINE AND STILL CONDITIONAL, which
        // separates `startswith('record(')` from `'record(' in stmt` -- review
        // measured the looser form accepting this with `ok 1 verb(s) metered`.
        "guarded-inline             | 1 | first statement is",
      })
  void refusesAVerbThatIsNotMetered(String fixture, int expectedExit, String expectedText)
      throws Exception {
    Path file =
        repo()
            .resolve("buildSrc/src/test/resources/fault-store-fixtures")
            .resolve(fixture.trim() + ".java");
    assertThat(file).exists();

    Process p =
        ProcessSupport.builder("scripts/check-fault-store-records.sh", file.toString())
            .directory(repo().toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    int exit = p.waitFor();

    assertThat(exit).as("output was:%n%s", out).isEqualTo(expectedExit);
    // ⚠️ THE EXPECTED TEXT NAMES THE OFFENDING VERB WHERE THERE IS ONE, not
    // just "not metered" — review measured that every refusal prints the same
    // two phrases, so the rows' expectations were interchangeable and a gate
    // blaming the WRONG verb passed. `unmetered-in-the-middle` names `presign`,
    // which is the middle member.
    assertThat(out).as("output was:%n%s", out).contains(expectedText.trim());
  }

  /**
   * A crashing predicate FAILS rather than reporting `ok`.
   *
   * <p>⚠️ THE GUARD EXISTS BUT NOTHING EXERCISED IT, which review measured by
   * deleting the `|| {{ fail …; finish; }}` block and watching every other
   * assertion stay green. Command substitution swallows the exit status, so an
   * exception inside the heredoc leaves the missing-list empty and the script
   * falls through to `ok`. A later tidy — moving the predicate into
   * `scripts/fault_store_scan.py` to match the siblings, say — would drop the
   * guard silently.
   *
   * <p>⚠️ THE STUB EXITS NON-ZERO AFTER PRINTING A PLAUSIBLE COUNT, because a
   * partial write before a crash must not read as a clean result either.
   */
  @Test
  void refusesWhenThePredicateItselfCrashes() throws Exception {
    Path bin = java.nio.file.Files.createTempDirectory("no-python");
    Path stub = bin.resolve("python3");
    java.nio.file.Files.writeString(stub, "#!/bin/sh\necho 'EXAMINED 12'\nexit 3\n");
    stub.toFile().setExecutable(true);

    ProcessBuilder pb =
        ProcessSupport.builder("scripts/check-fault-store-records.sh").directory(repo().toFile());
    boolean windows = System.getProperty("os.name").toLowerCase().contains("win");
    String python3 = stub.toString();
    if (windows) {
      python3 = "/" + Character.toLowerCase(python3.charAt(0))
          + python3.substring(2).replace('\\', '/');
    }
    pb.environment().put("PYTHON3", python3);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("output was:%n%s", out).isEqualTo(1);
    assertThat(out).as("output was:%n%s", out).contains("the predicate did not run");
  }

  /**
   * EVERY unmetered verb is reported, not just the first.
   *
   * <p>⚠️ EVERY OTHER FIXTURE HAS EXACTLY ONE UNMETERED VERB, which made
   * `missing[:1]` invisible to the whole suite — review measured it surviving.
   * The cost is an operator's time again: a one-line failure is fixed, re-run,
   * and meets the next offender, so a dozen unmetered verbs become a dozen
   * commits.
   */
  @Test
  void reportsEVERYUnmeteredVerbRatherThanTheFirst() throws Exception {
    Path file =
        repo()
            .resolve("buildSrc/src/test/resources/fault-store-fixtures")
            .resolve("two-unmetered.java");
    Process p =
        ProcessSupport.builder("scripts/check-fault-store-records.sh", file.toString())
            .directory(repo().toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("output was:%n%s", out).isEqualTo(1);
    assertThat(out).as("output was:%n%s", out).contains("presign").contains("multipart");
  }

  /**
   * A gate that cannot find its subject FAILS rather than passing vacuously.
   *
   * <p>⚠️ THE FAILURE MODE THIS FORBIDS IS THE ONE `check-reviewed` HAS with
   * nothing staged: a green line that means "examined nothing". If
   * `FaultInjectingStore` is renamed or moved, this gate must say so rather than
   * report ok over a file that is not there.
   */
  @Test
  void refusesToPassVacuouslyWhenItsSubjectIsMissing() throws Exception {
    Process p =
        ProcessSupport.builder("scripts/check-fault-store-records.sh", "no/such/Store.java")
            .directory(repo().toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("output was:%n%s", out).isEqualTo(1);
    assertThat(out).contains("does not exist");
  }

  /**
   * The real store passes, which is the gate's actual subject.
   *
   * <p>⚠️ Kept BESIDE the fixture cases rather than instead of them: this one
   * would pass today whatever the script did with a missing `record(...)`,
   * because nothing in the real file is missing one.
   */
  @Test
  void theRealFaultInjectingStoreIsFullyMetered() throws Exception {
    Process p =
        ProcessSupport.builder("scripts/check-fault-store-records.sh")
            .directory(repo().toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor()).as("output was:%n%s", out).isZero();
    // ⚠️ THE SUBJECT AND A FLOOR, NOT JUST "verb(s) metered". This is the ONLY
    // case in the tree that exercises the gate's DEFAULT target -- every
    // fixture row passes an explicit path -- and review measured the default
    // repointed at a two-verb fixture leaving all eight assertions green,
    // printing `ok 2 verb(s) metered`. A gate aimed anywhere at all would have
    // passed its own suite.
    assertThat(out)
        .as("the gate's default target is the fault-injecting store, not a fixture")
        .contains("FaultInjectingStore.java");
    // ⚠️ NOT ANCHORED ON "ok" -- `lib.sh` writes ANSI colour between the word
    // and the count, so an anchored pattern never matches and the assertion
    // reads as a failure rather than as a wrong number.
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("(\\d+) verb\\(s\\) metered").matcher(out);
    assertThat(m.find()).as("output was:%n%s", out).isTrue();
    // ⚠️ DERIVED FROM THE FILE, NOT A LITERAL. A floor would let "report double
    // the count" through (24 >= 12), and a hardcoded 12 would red the day a
    // verb is legitimately added. Counting the @Override lines here makes the
    // gate's own number checkable against its subject, which is what turns
    // three SCOPE mutations — examine only the first, skip past line 100,
    // double the count — from survivors into failures.
    long overrides =
        java.nio.file.Files.readAllLines(
                repo().resolve("sequencer/src/test/java/io/github/huyz0/os/biningester/sequencer/FaultInjectingStore.java"))
            .stream()
            .filter(l -> l.strip().startsWith("@Override"))
            .count();
    assertThat(Integer.parseInt(m.group(1)))
        .as("the gate reports the number it examined, and that is every verb")
        .isEqualTo((int) overrides);
  }
}
