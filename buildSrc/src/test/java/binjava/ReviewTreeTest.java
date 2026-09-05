// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * An isolated copy of the STAGED tree, one per reviewer (M0.57).
 *
 * <p>⚠️ THE TWO REVIEWERS VERIFY BY MUTATING — apply a change to production
 * code, run the suite, see which test fails, restore. That is not incidental:
 * nearly every major finding in M4 was found that way, and a read-only reviewer
 * reasoning over a diff would have found almost none of them. Run in ONE tree
 * they corrupt each other's evidence, and both {@code ./gradlew test} into the
 * same {@code build/}, overwriting the {@code test-results} XML that
 * {@code tdd_scan} and every pass/fail count read.
 *
 * <p>⚠️ THE LOAD-BEARING ASSERTION IS "STAGED", not "committed" and not
 * "worktree", and it is why {@code git worktree add} is the wrong primitive: it
 * checks out the COMMIT and carries no index state, so each reviewer would
 * review a tree WITHOUT the diff under review. {@code git checkout-index}
 * materialises exactly the bytes the verdict is bound to by hash.
 */
class ReviewTreeTest {

  /**
   * ⚠️ EVERY INVOCATION GETS ITS OWN TMPDIR. Round-2 review measured this
   * fixture landing 132 directories and 47M in the shared
   * {@code /tmp/binjava-review-trees}, mixed in beside a live reviewer's tree —
   * a suite that litters the directory the thing under test uses in anger.
   */
  @TempDir Path sandboxTmp;

  /**
   * A repository whose three versions of one file all differ, so a tree can be
   * asked which one it holds: committed {@code C}, staged {@code S}, worktree
   * {@code W}.
   */
  private Path scratch(Path dir) throws Exception {
    Files.createDirectories(dir.resolve("scripts"));
    Path repo = Path.of("..").toAbsolutePath().normalize();
    for (String f : List.of("review-tree.sh", "lib.sh", "check-file-size.sh")) {
      Path dst = dir.resolve("scripts").resolve(f);
      Files.copy(repo.resolve("scripts").resolve(f), dst);
      dst.toFile().setExecutable(true);
    }
    Files.writeString(dir.resolve("subject.txt"), "C\n");
    run(dir, "git init -q . && git config user.email t@e && git config user.name t"
        + " && git add -A && git commit -qm base");
    // ⚠️ Staged and then further modified: the three states are now distinct.
    Files.writeString(dir.resolve("subject.txt"), "S\n");
    run(dir, "git add -- subject.txt");
    Files.writeString(dir.resolve("subject.txt"), "W\n");
    return dir;
  }

  private String tree(Path dir, String role) throws Exception {
    Process p = hermetic(dir, List.of("bash", "scripts/review-tree.sh", role)).start();
    String out = new String(p.getInputStream().readAllBytes()).trim();
    assertThat(p.waitFor()).as("review-tree.sh " + role + " -> " + out).isZero();
    return out;
  }

  private void run(Path dir, String script) throws Exception {
    Process p = hermetic(dir, List.of("bash", "-c", "set -o pipefail; " + script))
        .redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(script + "\n" + out).isZero();
  }

  /** ⚠️ Children run with NO inherited git state — see {@link GitEnv}. */
  private ProcessBuilder hermetic(Path dir, List<String> command) {
    ProcessBuilder b = GitEnv.stripped(new ProcessBuilder(command).directory(dir.toFile()));
    b.environment().put("TMPDIR", sandboxTmp.toString());
    return b;
  }

  @Test
  void strippingRemovesEveryInheritedGitVariable() {
    // ⚠️ WITHOUT THIS TEST the strip is unfalsifiable: every other test here
    // passes whether or not it happens, because the JVM running them usually
    // has no GIT_* set -- the pollution only appears when the suite is invoked
    // from a shell that DOES, which is exactly the case the gate loop creates
    // and exactly the case no test would otherwise cover.
    ProcessBuilder b = new ProcessBuilder("true");
    b.environment().put("GIT_INDEX_FILE", "/tmp/decoy-index");
    b.environment().put("GIT_DIR", "/tmp/decoy.git");
    b.environment().put("GIT_WORK_TREE", "/tmp/decoy-tree");
    // ⚠️ NOT one of the three the name suggests. GIT_OBJECT_DIRECTORY
    // redirects a child's git the same way, and an explicit three-key
    // list -- which round-1 test review showed passes every other
    // assertion here -- would leave it inherited.
    b.environment().put("GIT_OBJECT_DIRECTORY", "/tmp/decoy-objects");
    // ⚠️ AN ARBITRARY KEY, because the four above constrain only themselves:
    // round-3 test review measured `removeIf` replaced by a `removeAll` of
    // exactly those four passing every assertion here, leaving GIT_COMMON_DIR
    // (which git exports inside a worktree), GIT_ALTERNATE_OBJECT_DIRECTORIES
    // and GIT_CEILING_DIRECTORIES inherited. Only a name no list could contain
    // pins the PREDICATE rather than the enumeration.
    b.environment().put("GIT_ZZZ_UNKNOWN_TO_ANY_LIST", "1");

    GitEnv.stripped(b);

    assertThat(b.environment().keySet())
        .as("an inherited index, dir, work tree or object directory would "
            + "redirect this fixture's own git writes onto the caller's state")
        .noneMatch(k -> k.startsWith("GIT_"));
  }

  @Test
  void theTreeHoldsTheSTAGEDBytesNotTheCommittedOnesAndNotTheWorktreeOnes(@TempDir Path dir)
      throws Exception {
    // ⚠️ THE ASSERTION THAT MATTERS. `git worktree add <path> HEAD` yields "C"
    // -- a tree without the diff under review, which is worse than no
    // isolation because it looks like a review and is not. A plain `cp -r`
    // yields "W", the author's uncommitted afterthoughts, which the verdict's
    // hash does not cover. Only the staged bytes are what the verdict binds
    // to, so only "S" is correct.
    Path repo = scratch(dir);
    Path materialised = Path.of(tree(repo, "reviewer"));

    assertThat(Files.readString(materialised.resolve("subject.txt")))
        .as("the tree must hold exactly the bytes the verdict is bound to")
        .isEqualTo("S\n");
  }

  @Test
  void eachRoleGetsItsOwnTreeSoConcurrentReviewersCannotCorruptEachOther(@TempDir Path dir)
      throws Exception {
    // ⚠️ M4.3 measured the failure this prevents: two reviewers mutating one
    // tree, each seeing the other's mutation as evidence.
    Path repo = scratch(dir);
    Path a = Path.of(tree(repo, "reviewer"));
    Path b = Path.of(tree(repo, "test-reviewer"));

    assertThat(a).as("two roles, two trees").isNotEqualTo(b);
    // A mutation by one role must be invisible to the other.
    Files.writeString(a.resolve("subject.txt"), "MUTATED BY reviewer\n");
    assertThat(Files.readString(b.resolve("subject.txt")))
        .as("the other reviewer's evidence is untouched by this one's mutation")
        .isEqualTo("S\n");
  }

  @Test
  void thePacketSTILLIssuesTheTreeSoTheScriptCannotBecomeAnOrphan() throws Exception {
    // ⚠️ Round-1 test review deleted the whole `YOUR OWN TREE` block from
    // `review.sh` and every test here stayed green -- so the wiring could be
    // removed and the behaviour would silently revert to the per-reviewer
    // improvisation this row exists to replace.
    // ⚠️ A GREP, and said plainly rather than dressed up: the behavioural
    // version runs `review.sh context`, which runs the whole gate loop
    // including the Gradle suites, and cannot be built in a scratch repository
    // that has neither. This pins that the packet still names the script.
    String packetSource = Files.readString(
        Path.of("..").toAbsolutePath().normalize().resolve("scripts/review.sh"));
    assertThat(packetSource)
        .as("the packet must keep issuing the tree, or reviewers improvise again")
        .contains("review-tree.sh");
  }

  @Test
  void theTreeIsMaterialisedFromTheCALLERSPrivateIndexNotTheRepositorysDefaultOne(
      @TempDir Path dir) throws Exception {
    // ⚠️ THE PROPERTY THE WHOLE SHARED-CHECKOUT WORKFLOW RESTS ON, and round-1
    // test review measured that it had ZERO coverage: inserting
    // `unset GIT_INDEX_FILE` before `checkout-index` left every other test in
    // this file green. Without it a reviewer in a shared checkout is handed
    // ANOTHER session's staged bytes, cannot tell, and produces a verdict
    // hash-bound to bytes it never saw.
    // ⚠️ THE ONE TEST HERE THAT MUST *NOT* BE HERMETIC. `hermetic()` strips
    // GIT_* from every child, which is what makes the rest of this suite safe
    // -- and is also what made this path unreachable from a test. This one
    // passes GIT_INDEX_FILE deliberately, because that is the input under test.
    Path repo = scratch(dir);
    run(repo, "git add -- subject.txt");            // default index: "W"
    Path privateIndex = dir.resolve("private.idx");
    run(repo, "printf 'PRIVATE\\n' > subject.txt"
        + " && GIT_INDEX_FILE=" + privateIndex + " git read-tree HEAD"
        + " && GIT_INDEX_FILE=" + privateIndex + " git add -- subject.txt");

    // ⚠️ GIT_DIR AND GIT_WORK_TREE TOO, because the script's own `GIT_*` strip
    // was otherwise DEAD CODE under test: round-3 test review deleted the unset
    // loop and all eleven tests stayed green. The strip is what stops the tree's
    // `git init`/`commit`/`add` from running against the CALLER'S repository --
    // measured without it, the caller's HEAD gained a commit titled for the
    // review tree and its index grew. Only a non-hermetic child that exports
    // these can reach it, and this is the one test here that must not be
    // hermetic.
    String headBefore = capture(repo, "git rev-parse HEAD");
    String stagedBefore = capture(repo, "git diff --cached --name-only | wc -l");

    ProcessBuilder b = hermetic(repo, List.of("bash", "scripts/review-tree.sh", "reviewer"));
    b.environment().put("GIT_INDEX_FILE", privateIndex.toString());
    b.environment().put("GIT_DIR", repo.resolve(".git").toString());
    b.environment().put("GIT_WORK_TREE", repo.toString());
    // ⚠️ A FOURTH KEY, and it is the one that makes this the PREDICATE rather
    // than an enumeration. Round-4 test review measured the shell strip
    // replaced by `unset GIT_INDEX_FILE GIT_DIR GIT_WORK_TREE` -- exactly the
    // three above -- passing every test here, while the tree's `.git` ended up
    // with no `objects/` at all and the caller's object store grew instead. A
    // tree like that is not a repository anywhere outside this environment.
    b.environment().put("GIT_OBJECT_DIRECTORY", repo.resolve(".git/objects").toString());
    Process p = b.start();
    String materialised = new String(p.getInputStream().readAllBytes()).trim();
    assertThat(p.waitFor()).as("review-tree.sh -> " + materialised).isZero();

    assertThat(Files.readString(Path.of(materialised).resolve("subject.txt")))
        .as("the caller's private index is what the verdict binds to, so it is "
            + "what must be materialised -- not the shared default index")
        .isEqualTo("PRIVATE\n");
    assertThat(capture(repo, "git rev-parse HEAD"))
        .as("the tree's own commit must land in the TREE, never in the caller's "
            + "repository -- an unstripped GIT_DIR sends it here")
        .isEqualTo(headBefore);
    assertThat(capture(repo, "git diff --cached --name-only | wc -l"))
        .as("and the caller's index must be untouched")
        .isEqualTo(stagedBefore);
    try (var objects = Files.list(Path.of(materialised).resolve(".git/objects"))) {
      assertThat(objects.findAny())
          .as("the tree must own its objects; an inherited GIT_OBJECT_DIRECTORY "
              + "sends them to the caller's store and leaves this empty")
          .isPresent();
    }
  }

  @Test
  void aSYMLINKEDParentUnderTmpdirCannotLandTheTreeInsideTheRepository(@TempDir Path dir)
      throws Exception {
    // ⚠️ RESOLVING TMPDIR IS NOT ENOUGH: the script appends one fixed component
    // to it, and `mkdir -p` and `mktemp -d` both follow that if it is a symlink.
    // Round-4 review reproduced a pre-existing symlink at exactly that
    // predictable name, in a world-writable directory, landing the whole tree
    // and a nested `.git` physically inside the working tree while printing an
    // outside-looking path -- where a recursive delete of test-results XML
    // reaches a live reviewer's evidence.
    Path repo = scratch(Files.createDirectories(dir.resolve("repo")));
    Path inside = Files.createDirectories(repo.resolve("landing-zone"));
    Path tmp = Files.createDirectories(dir.resolve("outside-tmp"));
    Files.createSymbolicLink(tmp.resolve("binjava-review-trees"), inside);

    ProcessBuilder b = hermetic(repo, List.of("bash", "scripts/review-tree.sh", "reviewer"));
    b.environment().put("TMPDIR", tmp.toString());
    Process p = b.redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes()).trim();

    assertThat(p.waitFor())
        .as("a symlinked parent resolving into the repository must be refused; "
            + "it said: " + out)
        .isNotZero();
    try (var entries = Files.list(inside)) {
      assertThat(entries.findAny())
          .as("and nothing may be materialised inside the repository at " + inside)
          .isEmpty();
    }
  }

  /** The trimmed stdout of a shell command run in {@code dir} with no git state. */
  private String capture(Path dir, String script) throws Exception {
    Process p = hermetic(dir, List.of("bash", "-c", script)).start();
    String out = new String(p.getInputStream().readAllBytes()).trim();
    assertThat(p.waitFor()).as(script + " -> " + out).isZero();
    return out;
  }

  @Test
  void aRoleThatIsNotOneOfTheTwoIsREFUSEDBeforeAnythingIsWritten(@TempDir Path dir)
      throws Exception {
    // ⚠️ ROLE LANDS IN A FILESYSTEM PATH. Round-1 test review deleted a file
    // outside the tree with `review-tree.sh '../../../victim'` back when this
    // script removed its destination first, and measured that deleting the
    // whole validation block left every test green. The `rm -rf` is gone, but
    // an unvalidated role still materialises a repository somewhere nobody
    // asked for -- and an empty one collapses both roles onto one path,
    // reinstating exactly the collision this row exists to prevent.
    Path repo = scratch(dir);
    Path victim = dir.resolve("victim.txt");
    Files.writeString(victim, "not yours to touch\n");

    for (String role : List.of("../../victim", "", "REVIEWER", "reviewer extra")) {
      Process p = hermetic(repo, List.of("bash", "scripts/review-tree.sh", role))
          .redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes()).trim();
      assertThat(p.waitFor()).as("role <" + role + "> must be refused; got: " + out).isNotZero();
    }
    assertThat(victim).as("nothing outside the tree was touched").exists();
    assertThat(Files.readString(victim)).isEqualTo("not yours to touch\n");
    // ⚠️ "BEFORE ANYTHING IS WRITTEN" is in the name, so it is asserted:
    // round-2 review moved the allowlist below the directory creation and
    // every test stayed green, leaving a tree materialised for a role the
    // script had just rejected.
    try (var entries = Files.list(sandboxTmp)) {
      assertThat(entries.findAny())
          .as("a refused role must not have created anything under TMPDIR")
          .isEmpty();
    }
  }

  @Test
  void theTreeIsAGitRepositorySoGatesDoNotPassVACUOUSLYInsideIt(@TempDir Path dir)
      throws Exception {
    // ⚠️ ROUND-1 REVIEW MEASURED THE FAILURE THIS PREVENTS, and it is the
    // worst kind: silent green. Every per-file gate resolves its input through
    // lib.sh's `workspace_files`/`scoped_files`, which ask git. In a tree with
    // no `.git` they find nothing and report "0 source file(s) within 500
    // lines" -- a PASS. A reviewer verifying a gate change the intended way,
    // by mutating the input the gate must reject, would get green for every
    // mutation. That is the packet's own first lens turned on the review
    // itself. `FreshCheckoutTest` fails there for the same reason: 13 of its
    // tests die on `fatal: not a git repository`.
    // ⚠️ ASSERTED THROUGH A REAL GATE'S EXIT CODE, not through `git ls-files`.
    // Round-3 test review measured why that distinction is the finding: gates
    // default to DELTA mode, whose input is `git diff --cached`, and a draft
    // that COMMITTED the materialised tree left index == HEAD, so the delta was
    // empty and `check-file-size` printed the exact vacuous string quoted above
    // in a tree holding a deliberately over-long file. `git ls-files` is
    // `GATE_SCOPE=full`'s input and stayed happily green throughout. So the
    // input here is a staged over-limit file, and the assertion is that the
    // gate REFUSES it.
    Path repo = scratch(dir);
    StringBuilder big = new StringBuilder("package binjava;\nclass Big {\n");
    for (int i = 0; i < 600; i++) {
      big.append("  // a line past the limit, ").append(i).append('\n');
    }
    Files.createDirectories(repo.resolve("mod/src/main/java/binjava"));
    Files.writeString(repo.resolve("mod/src/main/java/binjava/Big.java"), big.append("}\n"));
    run(repo, "git add -- mod/src/main/java/binjava/Big.java");
    Path materialised = Path.of(tree(repo, "reviewer"));

    Process p = hermetic(materialised, List.of("bash", "scripts/check-file-size.sh"))
        .redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());

    assertThat(p.waitFor())
        .as("a gate run inside the tree must SEE the staged files; it said:\n" + out)
        .isNotZero();
    assertThat(out)
        .as("and must name the file it refused, not merely count zero of them:\n" + out)
        .contains("Big.java");
  }

  @Test
  void aMaterialisationThatCouldNotCompleteFAILSRatherThanPrintingAPath(@TempDir Path dir)
      throws Exception {
    // ⚠️ `lib.sh` sets `set -uo pipefail`, NOT `set -e`, so an unchecked
    // `git checkout-index` that only half-wrote the tree still let the path
    // reach stdout and the script still exited 0 -- and `TREE=$(...) && cd
    // "$TREE"` then succeeded into a tree that is not the staged bytes. A
    // reviewer would review the wrong files and never know.
    Path repo = scratch(dir);
    // An index entry whose blob does not exist: checkout-index cannot write it.
    run(repo, "git update-index --add --cacheinfo 100644,"
        + "0000000000000000000000000000000000000001,phantom.txt");

    Process p = hermetic(repo, List.of("bash", "scripts/review-tree.sh", "reviewer")).start();
    String out = new String(p.getInputStream().readAllBytes()).trim();
    assertThat(p.waitFor())
        .as("a partial materialisation must fail loudly; it printed: " + out)
        .isNotZero();
  }

  @Test
  void asecondInvocationDoesNotDESTROYATreeAlreadyInUse(@TempDir Path dir) throws Exception {
    // ⚠️ MEASURED DURING THIS ROW'S OWN ROUND-1 REVIEW: the reviewer's tree was
    // removed and re-created underneath it, mid-review, by an unrelated
    // invocation for the same role -- a `cp` out of it failed with "No such
    // file or directory". That is the M4.3 evidence corruption this row exists
    // to prevent, re-created one level up and with an `rm -rf` instead of an
    // overwrite.
    Path repo = scratch(dir);
    Path first = Path.of(tree(repo, "reviewer"));
    Files.writeString(first.resolve("in-flight-evidence.txt"), "a mutation mid-review\n");

    Path second = Path.of(tree(repo, "reviewer"));

    assertThat(second).as("a second call must not hand back the same directory").isNotEqualTo(first);
    assertThat(first.resolve("in-flight-evidence.txt"))
        .as("the in-flight tree must survive another invocation for the same role")
        .exists();
  }

  @Test
  void aTmpdirInsideTheRepositoryIsREFUSEDAndLeavesNothingBehind(@TempDir Path dir)
      throws Exception {
    // ⚠️ Round-2 review deleted the whole containment guard and every test
    // here stayed green, because no fixture ever set TMPDIR -- the script says
    // "TMPDIR is the caller's, so this is an input", and the input was never
    // exercised. It also refused AFTER creating, so a rejected TMPDIR still
    // left directories inside the working tree.
    // ⚠️ THE DIRECTORY IS CREATED, and a draft of this test did not create it —
    // so the script refused on the EARLIER "TMPDIR is not a directory" branch
    // and the containment guard was never reached. Round-3 test review measured
    // the consequence: deleting the whole guard, and swapping `pwd -P` for
    // `pwd`, both left this file green.
    // ⚠️ THE REPOSITORY IS BUILT UNDER A SUBDIRECTORY AND ALSO REACHED THROUGH A
    // SYMLINK, because a physical/logical mismatch needs somewhere to hide on
    // BOTH sides of the comparison and a fixture rooted at the temp dir itself
    // gives it nowhere: every path is then literally under the repository, so a
    // logical resolution agrees with a physical one and both `pwd -P` mutations
    // survive. Measured.
    Path repo = scratch(Files.createDirectories(dir.resolve("repo")));
    Path inside = Files.createDirectories(repo.resolve("scratch-inside-repo"));
    Path linkToInside = Files.createSymbolicLink(dir.resolve("looks-outside"), inside);
    Path linkToRepo = Files.createSymbolicLink(dir.resolve("link-to-repo"), repo);

    // cwd, TMPDIR, and what each one exercises.
    List<List<Object>> cases = List.of(
        List.of(repo, inside, "a real directory inside the repository"),
        // Under a plain `pwd` this TMPDIR does not look like it is inside the
        // repository, so the guard waves through a full copy and a nested
        // `.git` physically into the working tree.
        List.of(repo, linkToInside, "a symlink from outside pointing back in"),
        // The mirror image: the REPOSITORY is reached through a symlink, so
        // lib.sh's plain-`pwd` ROOT is logical while TMPDIR is physical. Only
        // resolving both sides catches it.
        List.of(linkToRepo, inside, "a repository reached through a symlink"));

    for (List<Object> c : cases) {
      ProcessBuilder b =
          hermetic((Path) c.get(0), List.of("bash", "scripts/review-tree.sh", "reviewer"));
      b.environment().put("TMPDIR", c.get(1).toString());
      // ⚠️ PWD IS SET THE WAY A SHELL SETS IT. ProcessBuilder chdirs, and bash
      // then takes PWD from getcwd(), which is always PHYSICAL -- so lib.sh's
      // logical `pwd` silently agreed with a physical one and the repository
      // side of the guard could not be exercised from a test at all. A person
      // who `cd`s through a symlink and runs a gate has the logical path in
      // PWD, and bash honours it; that is the case being reproduced.
      b.environment().put("PWD", c.get(0).toString());
      Process p = b.redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes()).trim();

      assertThat(p.waitFor())
          .as(c.get(2) + " must be refused; it said: " + out)
          .isNotZero();
      try (var entries = Files.list(inside)) {
        assertThat(entries.findAny())
            .as(c.get(2) + ": refusing it must leave nothing behind under " + inside)
            .isEmpty();
      }
    }
  }

  @Test
  void theTreeLivesOUTSIDETheRepositorySoNoGateEverScansIt(@TempDir Path dir) throws Exception {
    // ⚠️ NOT "the gates would scan it" -- round-2 review measured that claim
    // false, twice, because check-file-size and check-links resolve their
    // input through lib.sh's git-derived `scoped_files`. What a copy inside
    // the tree really costs: anything that WALKS the directory rather than
    // asking git picks it up -- check-terminology.sh and
    // check-metric-cardinality.sh both `grep -r`, and tdd-red.sh does
    // `find . -delete` -- and it leaves a stale second copy of every source
    // file for a human or a grep to read as if it were the real one.
    Path repo = scratch(dir);
    Path materialised = Path.of(tree(repo, "reviewer"));

    assertThat(materialised.startsWith(repo.toAbsolutePath().normalize()))
        .as("materialised at " + materialised + ", which must not be under " + repo)
        .isFalse();
  }
}
