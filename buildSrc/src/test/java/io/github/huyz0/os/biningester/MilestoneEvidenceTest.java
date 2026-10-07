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

  private static final String EVIDENCED =
      "## Acceptance criteria\n1. FirstTest#first.\n2. NOT-RUN: needs a cluster.\n";

  private static String harvestSpec(String harvestCell) {
    return "M collects:\n\n| Obligation | Assigned by |\n|---|---|\n"
        + "| " + harvestCell + " | M12/VERIFIED.md |\n| Fast mode | roadmap |\n\n" + SPEC;
  }

  private static String verifiedEnumerating(String... ids) {
    StringBuilder table = new StringBuilder(EVIDENCED
        + "\n## Harvest dispositions\n\n| Harvest | Disposition |\n|---|---|\n");
    for (String id : ids) {
      table.append("| ").append(id).append(" (M13.9) | closed: SomeTest |\n");
    }
    return table.toString();
  }

  /**
   * ⚠️ M13.40: criterion 6 asks VERIFIED.md to enumerate every harvest row once,
   * as M12's criterion 17 did -- and nothing checked the enumeration was whole,
   * so a row dropped from it closed the milestone without a disposition.
   */
  @Test
  void aHARVESTIdMissingFromTheEnumerationIsRefused() {
    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R2, R3"), verifiedEnumerating("R1", "R3")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("harvest enumeration is missing R2");
  }

  @Test
  void anENUMERATIONMissingAltogetherMissesEveryId() {
    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R2"), EVIDENCED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing R1, R2");
  }

  @Test
  void aWHOLEEnumerationPasses() {
    assertThat(MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R2, R3"), verifiedEnumerating("R3", "R1", "R2"))).isEmpty();
  }

  /**
   * ⚠️ ONLY IDS LISTED INDIVIDUALLY BIND: a range names none of its members
   * (M12's "H1–H25"), and a parenthetical is commentary (M13's "(R19 dropped
   * below)", whose disposition is the SPEC's own).
   */
  @Test
  void aRANGEOrAParentheticalListsNoIdIndividually() {
    assertThat(MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest H1–H25 (H26 dropped below)"), EVIDENCED)).isEmpty();
    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R6–R18, R19 (R20 dropped)"), verifiedEnumerating("R1")))
        .hasMessageContaining("missing R19");
  }

  /**
   * ⚠️ M13.40 review P1, T1: a closing VERIFIED.md carries more than one
   * milestone's harvest, and IDs repeat across milestones -- two tables headed
   * Harvest leave the gate guessing which is the enumeration, so it refuses.
   */
  @Test
  void aSECONDHarvestTableIsRefused() {
    String two = verifiedEnumerating("R1")
        + "\n### Harvest for the specification of M14\n\n| Harvest | Proposed |\n|---|---|\n"
        + "| R2 | a row for M14 |\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R2"), two))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than one harvest enumeration");
  }

  @Test
  void aFINDINGIdDoesNotStandInForAHarvestId() {
    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R3"), verifiedEnumerating("R1", "R3-P1")))
        .hasMessageContaining("missing R3");
  }

  @Test
  void aNONHarvestRowOfTheObligationsTableBindsNothing() {
    String spec = "M collects:\n\n| Obligation | Assigned by |\n|---|---|\n"
        + "| Open rows M12.27, M12.28 | M12/VERIFIED.md |\n"
        + "| Harvest R1 | M12/VERIFIED.md |\n\n" + SPEC;

    assertThat(MilestoneEvidence.INSTANCE.unevidenced(spec, verifiedEnumerating("R1")))
        .as("the open rows are no harvest IDs").isEmpty();
  }

  @Test
  void aHYPHENRangeOrABareEndpointNamesNoneOfItsMembers() {
    // ⚠️ M13.40 review T2: R6 is NOT enumerated, so a bare-endpoint range left
    // unstripped would bind it.
    assertThat(MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1-R5, R6–18, R7"), verifiedEnumerating("R7"))).isEmpty();
  }

  /**
   * ⚠️ M13.40 review T1: only a table HEADED Harvest is the enumeration -- a
   * findings table whose rows happen to start with the same IDs is not.
   */
  @Test
  void aTABLENotHeadedHarvestIsNoEnumeration() {
    String findings = EVIDENCED + "\n## Findings\n\n| Finding | Disposition |\n|---|---|\n"
        + "| R1 | closed |\n| R2 | closed |\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1, R2"), findings))
        .hasMessageContaining("missing R1, R2");
  }

  @Test
  void EMPHASISOrCaseDoesNotHideATableOrARow() {
    String spec = "M collects:\n\n| **OBLIGATION** | Assigned by |\n|---|---|\n"
        + "| **HARVEST R1, R2** | M12/VERIFIED.md |\n\n" + SPEC;
    String verified = EVIDENCED + "\n| **harvest** | Disposition |\n|---|---|\n| **R1** | closed |\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(spec, verified))
        .hasMessageContaining("missing R2")
        .hasMessageNotContaining("R1");
  }

  @Test
  void aSPECBindingNothingLeavesTheEnumerationsUnread() {
    String two = verifiedEnumerating("R1") + "\n| Harvest | Proposed |\n|---|---|\n| R2 | x |\n";

    assertThat(MilestoneEvidence.INSTANCE.unevidenced(harvestSpec("Harvest H1–H25"), two))
        .as("M12's SPEC binds no ID, so its VERIFIED.md's tables are not judged").isEmpty();
  }

  @Test
  void aTABLEInACodeFenceIsNoEnumeration() {
    String fenced = EVIDENCED + "\n```\n| Harvest | Disposition |\n|---|---|\n| R1 | x |\n```\n";

    assertThatThrownBy(() -> MilestoneEvidence.INSTANCE.unevidenced(
        harvestSpec("Harvest R1"), fenced))
        .hasMessageContaining("missing R1");
  }

  /** ⚠️ M13.40 review T3: what M13's own SPEC binds its close to. */
  @Test
  void M13sSPECListsR1ToR18() throws Exception {
    String spec = Files.readString(ROOT.resolve("docs/internal/product/milestones/M13/SPEC.md"));
    java.util.List<String> eighteen = new java.util.ArrayList<>();
    for (int i = 1; i <= 18; i++) {
      eighteen.add("R" + i);
    }

    assertThat(MilestoneEvidence.INSTANCE.unenumerated(spec, EVIDENCED))
        .containsExactlyElementsOf(eighteen);
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
