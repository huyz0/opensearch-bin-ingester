// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * {@code checkMilestoneVerified} reads evidence from VERIFIED.md's criteria
 * section only, and its default milestone is pinned (M11.20, H15).
 */
class MilestoneEvidenceTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final String SPEC = "## Acceptance criteria\n"
      + "1. first behaviour\n2. second behaviour\n\n## Design\n";

  @Test
  void aNumberedListBelowTheCriteriaDoesNotAnswerForADeletedCriterion() {
    String verified = "# M evidence\n\n## Acceptance criteria\n\n"
        + "1. FirstTest#first proves the first behaviour.\n\n"
        + "## Milestone review\n\n"
        + "1. A finding, fixed by FindingTest.\n"
        + "2. Another finding, fixed by OtherTest.\n";

    assertThat(MilestoneEvidence.INSTANCE.unevidenced(SPEC, verified))
        .as("⚠️ criterion 2's line was deleted: the review's `2.` is not its evidence")
        .containsExactly("2");
  }

  /**
   * ⚠️ M12.14 (H15): with no criteria heading the gate used to read the text
   * before the first section as the criteria -- so a VERIFIED.md that lost its
   * heading was judged on whatever preamble was left, and one whose evidence
   * all sat below a differently-named heading answered for nothing it could
   * see. Neither is a VERIFIED.md the gate can read; it refuses rather than
   * guesses.
   */
  @Test
  void aVerifiedFileWithNoCriteriaSectionIsRefused() {
    String verified = "# M verified\n\n1. FirstTest#first proves it.\n\n"
        + "## The unwired set\n\n2. SecondTest#second, in another section.\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(SPEC, verified))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VERIFIED.md has no Acceptance criteria section");
  }

  /**
   * ⚠️ M12.14 (H15): the first {@code N. } line answered for criterion N, so a
   * second line numbered N -- a pasted row, a renumbering left half done -- was
   * never read, and the criterion it was meant to answer for went unchecked.
   */
  @Test
  void aCriterionNumberedTwiceInTheCriteriaSectionIsRefused() {
    String verified = "## Acceptance criteria\n1. FirstTest#first.\n"
        + "1. SecondTest#second, meant for criterion 2.\n"
        + "2. NOT-RUN: needs a cluster.\n\n## Milestone review\n1. prose\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(SPEC, verified))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VERIFIED.md numbers criterion 1 more than once");
  }

  @Test
  void everyCriterionEvidencedInItsSectionPasses() {
    String verified = "## Acceptance criteria\n1. FirstTest#first.\n"
        + "2. NOT-RUN: needs a cluster.\n\n## Milestone review\n1. prose\n";

    assertThat(MilestoneEvidence.INSTANCE.unevidenced(SPEC, verified)).isEmpty();
  }

  /**
   * ⚠️ DERIVED, NOT A LITERAL: the gate's default is the latest milestone
   * that has been closed -- the highest-numbered one holding a VERIFIED.md --
   * so the commit that closes a milestone must move it, and a literal pinned
   * here would only have to be edited alongside.
   */
  @Test
  void theDefaultMilestoneIsTheLatestOneWithAVerifiedFile() throws Exception {
    String build = Files.readString(ROOT.resolve("build.gradle.kts"));
    Matcher configured = Pattern.compile(
        "findProperty\\(\"milestoneDir\"\\)\\?\\.toString\\(\\) \\?: "
            + "\"docs/internal/product/milestones/M(\\d+)\"").matcher(build);
    assertThat(configured.find()).as("the default is where this test reads it").isTrue();
    int latest;
    try (Stream<Path> dirs = Files.list(ROOT.resolve("docs/internal/product/milestones"))) {
      latest = dirs.filter(d -> Files.isRegularFile(d.resolve("VERIFIED.md")))
          .map(d -> d.getFileName().toString())
          .filter(name -> name.matches("M\\d+"))
          .mapToInt(name -> Integer.parseInt(name.substring(1)))
          .max().orElseThrow();
    }

    assertThat(Integer.parseInt(configured.group(1))).isEqualTo(latest);
  }
}
