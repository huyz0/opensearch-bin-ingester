// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * ⚠️ WHAT A RED RECORD IS BOUND TO (M0.56). The record says "this test was
 * observed failing", and the binding decides when that observation goes stale.
 *
 * <p>⚠️ Binding it to the WHOLE FILE, which is what it was, is neither cheap
 * nor sound. Not cheap: adding one test — or fixing one comment — invalidates
 * every record in the file, and each must be re-earned against its own distinct
 * mutation. Measured on M4.5, 21 records were re-earned three times. Not sound
 * either: a helper in ANOTHER file can change with no record invalidated at all,
 * so the file boundary never was the correct unit.
 *
 * <p>⚠️ The unit is the test's own declaration PLUS the file's non-test content.
 * An unrelated test changing must not invalidate; a shared helper changing must.
 * Binding to the body alone would be cheaper still and would be a real
 * weakening — the point of this task is speed that costs no strength.
 */
class RedRecordBindingTest {

  @Test
  void anUnrelatedTestChangingDoesNotInvalidateThisTestsRecord() throws Exception {
    // ⚠️ ONE DIMENSION: `gamma` appears and nothing else moves.
    assertThat(key("binding-test-added.java", "binjava.fix.BindingFixture#alpha"))
        .as("adding or editing another test in the same file is not a change to this one")
        .isEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
  }

  @Test
  void aSharedHelperChangingDoesInvalidateEveryTestThatCouldUseIt() throws Exception {
    // ⚠️ The half that must NOT be lost. `helper()` is what both tests assert
    // through, so a test observed failing before it changed was observing
    // something else.
    assertThat(key("binding-helper-changed.java", "binjava.fix.BindingFixture#alpha"))
        .as("a same-file helper is part of what the test was observed doing")
        .isNotEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
  }

  @Test
  void thisTestsOwnBodyChangingInvalidatesIt() throws Exception {
    // ⚠️ The property the whole gate exists for, kept. ONE DIMENSION: only
    // `beta`'s own body moves, so this cannot pass because something else did.
    assertThat(key("binding-own-body-changed.java", "binjava.fix.BindingFixture#beta"))
        .as("what was observed failing is not what is being committed")
        .isNotEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#beta"));
  }

  /**
   * ⚠️ THE TWO NORMALISATIONS MUST AGREE, on every shape the parser accepts.
   *
   * <p>`scan` parses `normalise`; `scan_raw_spans` parses `normalise_lp`, whose
   * only reason to exist is preserving length so a span can slice the raw
   * source. If they disagree about which tests exist, a span slices the wrong
   * bytes and a record is bound to something other than the test it names —
   * a false green, not churn.
   *
   * <p>⚠️ Driven over EVERY fixture in the directory rather than over the six
   * written for this task, which contain no comment, text block, char literal,
   * unicode escape, annotation argument or nested class — none of what
   * `normalise_lp` exists to survive. Deleting its blanking step left all 117
   * tests green while the two disagreed on 11 real source files.
   */
  @ParameterizedTest(name = "normalisations agree on {0}")
  @MethodSource("everyFixture")
  void bothNormalisationsSeeTheSameTests(String fixture) throws Exception {
    List<String> ids = lines("ids", fixture);
    List<String> spans = lines("spans", fixture);
    assertThat(spans.stream().map(l -> l.split(" ")[0]).toList())
        .as("the span parser and the id parser must agree on %s", fixture)
        .containsExactlyElementsOf(ids);
    // ⚠️ COMPUTED HERE, not read from the `OK` the production code prints:
    // that verdict is the code under test grading itself, and hard-coding it to
    // `OK` passed every case. The offsets are on the line and the fixture is on
    // disk, so the test can check them.
    String src = Files.readString(Path.of("..").toAbsolutePath().normalize()
        .resolve("buildSrc/src/test/resources/java-fixtures").resolve(fixture));
    for (String line : spans) {
      String[] f = line.split(" ");
      int lo = Integer.parseInt(f[1]);
      int hi = Integer.parseInt(f[2]);
      assertThat(src.charAt(lo)).as("%s: span starts at an annotation", f[0]).isEqualTo('@');
      assertThat(src.charAt(hi - 1)).as("%s: span ends at a brace", f[0]).isEqualTo('}');
      // ⚠️ AND THE HEAD IS ONLY ANNOTATIONS AND A SIGNATURE. This is the
      // discriminating invariant; the `@`/`}` pair is not. EVERY wrong answer
      // in this family starts at some `@`, because a dangling annotation run
      // always points at one -- which is why each wrong span start cost its own
      // fixture pair and the set was one short every round. A `;`, a `}` or a
      // type keyword before the body means the span swallowed something
      // belonging to the shared remainder: a shared field, a nested class, an
      // annotation type. Verified over EVERY span this repo produces -- 647
      // spans in 112 files, zero violations -- so it is not a rule shaped to
      // these fixtures. ⚠️ But it is an invariant of THIS REPO'S annotation
      // style, not of Java. `@CsvSource(delimiter = ';')`, `@DisplayName("a
      // record is written")` and `@DisplayName("closes with }")` are each legal
      // and each violate a clause. It fails CLOSED -- a loud red, never a false
      // green -- so when one appears the repair is a NARROWER predicate, never
      // deleting it: that is the round-2 `@SuppressWarnings ("unchecked")`
      // shape, one level up.
      // ⚠️ Reach limit: the head is truncated at the FIRST `{`, so on
      // `@ValueSource(ints = {1, 2, 3})` only the text before that brace is
      // inspected. No mutation exploits it today.
      String head = src.substring(lo, hi).split("\\{")[0];
      assertThat(head)
          .as("%s %s: span head must be annotations and a signature only", fixture, f[0])
          .doesNotContain(";").doesNotContain("}")
          .doesNotMatch("(?s).*\\b(class|interface|enum|record)\\b.*");
    }
  }

  static List<String> everyFixture() throws IOException {
    Path dir = Path.of("..").toAbsolutePath().normalize()
        .resolve("buildSrc/src/test/resources/java-fixtures");
    try (var s = Files.list(dir)) {
      return s.map(p -> p.getFileName().toString())
          .filter(n -> n.endsWith(".java")).sorted().toList();
    }
  }

  private static List<String> lines(String cmd, String fixture)
      throws IOException, InterruptedException {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f = fixture.startsWith("/") ? Path.of(fixture) : fixtureDir().resolve(fixture);
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", cmd, f.toString())
            .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as("%s %s: %s", cmd, fixture, out).isZero();
    return out.lines().filter(l -> !l.isBlank()).toList();
  }

  /**
   * ⚠️ THE KEY IS COMPUTED INDEPENDENTLY, not compared with itself.
   *
   * <p>Asking production whether it agrees with production is a CONSISTENCY
   * test, and a consistency test is blind to any transform applied uniformly
   * to both sides — which is the defect class five review rounds re-found.
   * Hashing the raw declaration inside {@code binding_key}, or blanking text
   * blocks after {@code key_parts} returned, each survived a whole suite that
   * way.
   *
   * <p>⚠️ So this recomputes both halves from the raw source through the
   * {@code hashable} seam, checks they are what {@code key-parts} reports, and
   * then computes {@code sha256(decl + NUL + remainder)} here and checks THAT
   * is the key. Everything between {@code key_parts} and the digest is
   * constrained only by the last step.
   */
  @ParameterizedTest(name = "key of {0}")
  @MethodSource("everyFixture")
  void theKeyIsExactlyHashableOfBothHalvesJoined(String fixture) throws Exception {
    String src = Files.readString(fixtureDir().resolve(fixture));
    List<int[]> cuts = lines("spans", fixture).stream()
        .map(l -> new int[] {Integer.parseInt(l.split(" ")[1]), Integer.parseInt(l.split(" ")[2])})
        .sorted((x, y) -> Integer.compare(x[0], y[0])).toList();

    StringBuilder rem = new StringBuilder();
    int at = 0;
    for (int[] c : cuts) {
      append(rem, hashableOf(src.substring(at, c[0])));
      at = c[1];
    }
    append(rem, hashableOf(src.substring(at)));

    for (String line : lines("spans", fixture)) {
      String id = line.split(" ")[0];
      int lo = Integer.parseInt(line.split(" ")[1]);
      int hi = Integer.parseInt(line.split(" ")[2]);
      String decl = hashableOf(src.substring(lo, hi));

      List<String> parts = lines2("key-parts", fixture, id);
      assertThat(new String(java.util.Base64.getDecoder().decode(parts.get(0).substring(5)),
              java.nio.charset.StandardCharsets.UTF_8))
          .as("%s %s: the declaration half", fixture, id).isEqualTo(decl);
      assertThat(new String(java.util.Base64.getDecoder().decode(parts.get(1).substring(4)),
              java.nio.charset.StandardCharsets.UTF_8))
          .as("%s %s: the remainder half", fixture, id).isEqualTo(rem.toString());

      java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
      md.update(decl.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(rem.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte b : md.digest()) {
        hex.append(String.format("%02x", b));
      }
      assertThat(key(fixture, id))
          .as("%s %s: the key must BE sha256(decl + NUL + remainder)", fixture, id)
          .isEqualTo(hex.toString());
    }
  }

  private static void append(StringBuilder sb, String piece) {
    if (!piece.isEmpty()) {
      sb.append(sb.length() == 0 ? "" : "\n").append(piece);
    }
  }

  private static Path fixtureDir() {
    return Path.of("..").toAbsolutePath().normalize()
        .resolve("buildSrc/src/test/resources/java-fixtures");
  }

  private static String sha256(String s) throws Exception {
    byte[] d = java.security.MessageDigest.getInstance("SHA-256")
        .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    for (byte b : d) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private String hashableOf(String src) throws Exception {
    // ⚠️ Under build/tmp, not the system temp dir (testing.md rule 17).
    Path scratch = Path.of("build", "tmp", "hashable");
    Files.createDirectories(scratch);
    Path f = Files.createTempFile(scratch, "h", ".java");
    try {
      Files.writeString(f, src);
        // ⚠️ RAW stdout. Routing through `lines(...)` dropped blank lines, and
      // `hashable` keeps a text block's interior verbatim -- so a payload with
      // an empty line made this "independent" recomputation lossy and produced
      // a false RED against correct production code. The repair under pressure
      // is to loosen the byte comparison, which is the inversion the standard
      // forbids.
      Path repo = Path.of("..").toAbsolutePath().normalize();
      Process p = new ProcessBuilder("python3", "scripts/tdd_scan.py", "hashable",
              f.toAbsolutePath().toString())
          .directory(repo.toFile()).redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(),
          java.nio.charset.StandardCharsets.UTF_8);
      assertThat(p.waitFor()).as(out).isZero();
      return out;
    } finally {
      Files.deleteIfExists(f);
    }
  }

  private static List<String> lines2(String cmd, String fixture, String id)
      throws IOException, InterruptedException {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Process p = new ProcessBuilder("python3", "scripts/tdd_scan.py", cmd,
            fixtureDir().resolve(fixture).toString(), id)
        .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as("%s: %s", cmd, out).isZero();
    return out.lines().filter(l -> !l.isBlank()).toList();
  }

  private static String key(String fixture, String ident) throws IOException, InterruptedException {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f = repo.resolve("buildSrc/src/test/resources/java-fixtures").resolve(fixture);
    assertThat(f).exists();
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", "key", f.toString(), ident)
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes()).trim();
    assertThat(p.waitFor()).as("tdd_scan.py key %s %s: %s", fixture, ident, out).isZero();
    assertThat(out).as("a binding key is a hex digest").matches("[0-9a-f]{64}");
    return out;
  }

  @Test
  void anAnnotationAddedToTheTestInvalidatesIt() throws Exception {
    // ⚠️ THE DECLARATION IS HASHED, NOT JUST THE BODY. Without this, hashing
    // the body instead of the span passes every other test here -- and under
    // that mutation "earn a red, then add @Disabled before committing" is
    // invisible to BOTH gates: `check-tdd` sees an unchanged key, and
    // `check-test-integrity` skips a test that has no `before` because it was
    // added and disabled in the same commit. The whole-file binding this
    // replaces caught it, so losing it would be a straight regression.
    assertThat(key("binding-annotation-added.java", "binjava.fix.BindingFixture#alpha"))
        .as("annotations and signature are part of what was observed failing")
        .isNotEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
    // ⚠️ BETA is the discriminating half, and without it this test passes for
    // the wrong reason. If the span started at the ARMING annotation instead of
    // at the start of the annotation run, `@Disabled` would fall into the
    // shared remainder: alpha's key would still change -- so the assertion
    // above still passes -- while beta's changed too, which is the churn the
    // whole task removes, returning through a common JUnit idiom.
    assertThat(key("binding-annotation-added.java", "binjava.fix.BindingFixture#beta"))
        .as("and they belong to THAT test, not to the file everyone shares")
        .isEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#beta"));
  }

  @Test
  void aStringLiteralChangingInvalidatesTheRecord() throws Exception {
    // ⚠️ THE BLOCKING FINDING OF ROUND 1, kept closed. The first attempt hashed
    // the NORMALISED source, which blanks string bodies, char literals and text
    // blocks and strips annotation arguments -- so a test's entire assertion
    // surface could be rewritten after the red run with the key unchanged.
    // Review demonstrated it on a real NDJSON text block and on 11 of 12
    // @CsvSource rows. Offsets now index the raw source.
    // ⚠️ ALL FOUR SURFACES the blocking finding named -- string, char, text
    // block AND annotation argument -- and every one changed to the SAME
    // LENGTH. Covering only `string`, and only at a different length, left
    // three length-preserving reintroductions of that defect passing: blanking
    // char/textblock groups, and blanking annotation arguments, each
    // demonstrated on a real file in this repository.
    // ⚠️ ONE FIXTURE PER SURFACE PER HALF, each differing from the base in
    // exactly one dimension and at the SAME LENGTH. The declaration and the
    // remainder are two independent paths through `hashable`, so a fixture in
    // one proves nothing about the other -- every fixture through round 3
    // varied something inside alpha's declaration, while 121 of this repo's
    // 159 real test files carry a literal in the remainder. A combined fixture cannot isolate them: the
    // string difference alone satisfied the assertion, so blanking char, text
    // block or annotation arguments -- each a length-preserving reintroduction
    // of the round-1 blocking defect -- passed.
    for (String surface : List.of("binding-literal-changed.java", "binding-char-changed.java",
            "binding-textblock-changed.java", "binding-annarg-changed.java",
            "binding-literal-spacing.java", "binding-remainder-literal-spacing.java",
            "binding-remainder-textblock-changed.java")) {
      assertThat(key(surface, "binjava.fix.BindingFixture#alpha"))
          .as("%s: a literal is the assertion; blanking it blinds the binding", surface)
          .isNotEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
    }
  }

  @Test
  void aSharedFieldChangingInvalidatesEveryTestBesideIt() throws Exception {
    // ⚠️ BETA is the assertion, and it is what catches a wrong span START.
    // The base carries an ANNOTATED field; ten real files in this repo have one
    // (including HashableTest, added by this change) and no fixture did. With
    // the annotation-run reset dropped, the field is swallowed into the next
    // test's DECLARATION and leaves the shared remainder -- so renaming it left
    // beta's key unchanged, a false green, with the whole suite passing.
    // ⚠️ ALPHA, not beta: the field sits immediately before beta, so a wrong
    // span start swallows it into BETA's declaration -- where renaming it still
    // changes beta's key, and the mutation hides. It leaves ALPHA's remainder,
    // and that is the observation that discriminates.
    assertThat(key("binding-remainder-field-changed.java", "binjava.fix.BindingFixture#alpha"))
        .as("a shared field belongs to the remainder every OTHER test is bound to")
        .isNotEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
  }

  @Test
  void anAnnotationOnANestedClassBelongsToTheRemainder() throws Exception {
    // ⚠️ The other annotation-run reset, and it needs a NESTED CLASS: with the
    // reset on a brace deleted, `@Nested class Inner {` is swallowed into the
    // inner test's declaration and leaves the shared remainder -- so annotating
    // the nested class stops invalidating the tests beside it. No `binding-*`
    // fixture had a nested class, and `source_of`'s own docstring records that
    // this binding was got wrong for `@Nested` once already.
    assertThat(key("binding-nested-annotated.java", "binjava.fix.NestedFixture#outside"))
        .as("annotating a shared nested class is a change every test sees")
        .isNotEqualTo(key("binding-nested-base.java", "binjava.fix.NestedFixture#outside"));
  }

  @Test
  void aCommentInsideATestDeclarationDoesNotInvalidateIt() throws Exception {
    // ⚠️ The comment property was asserted for the REMAINDER half only, so
    // hashing the declaration raw survived. This repository's test bodies are
    // almost entirely comments, which makes the declaration half the dominant
    // case rather than the exotic one.
    assertThat(key("binding-declaration-comment.java", "binjava.fix.BindingFixture#alpha"))
        .as("a comment inside a test body is prose too")
        .isEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
  }

  @Test
  void aCommentChangingDoesNotInvalidateTheRecord() throws Exception {
    // ⚠️ THE OTHER HALF OF THE TASK, and it was delivered by a function no test
    // held: `hashable` could be replaced by `return src` with all 135
    // tests green, while the row asserted the behaviour as measured. A comment
    // cannot change what a test does, so observing it fail still stands.
    assertThat(key("binding-comment-changed.java", "binjava.fix.BindingFixture#alpha"))
        .as("prose is not code; editing it does not unmake the observation")
        .isEqualTo(key("binding-base.java", "binjava.fix.BindingFixture#alpha"));
  }

  @Test
  void askingForATestThatIsNotThereFailsRatherThanPrintingSomething() throws Exception {
    // ⚠️ testing.md rule 14. `binding_key` returns null when the parser cannot
    // see the id -- reachable through M0.17's composed-annotation blind spot --
    // and a caller that exits 0 on that would hand a record no bytes back it.
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Path f = repo.resolve("buildSrc/src/test/resources/java-fixtures/binding-base.java");
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", "key", f.toString(),
                "binjava.fix.BindingFixture#noSuchTest")
            .directory(repo.toFile())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as("must not exit 0: %s", out).isNotZero();
    assertThat(out).contains("no such test");
  }
}
