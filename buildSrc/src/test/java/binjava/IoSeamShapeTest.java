// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The shape half of non-negotiable 7's gate: HOW a banned name is matched.
 *
 * <p>⚠️ SPLIT OUT OF {@code IoSeamGateTest} when it crossed 700 lines --
 * code-structure.md rule 1, split rather than raise. Three files, three
 * questions: {@code IoSeamGateTest} asks what the gate DOES, {@code
 * IoSeamLexerTest} asks what counts as CODE, and this one asks whether a given
 * spelling of a name IS the banned thing -- across a line break, through a
 * method reference, behind a qualifier, and on a receiver that is a type rather
 * than a value.
 *
 * <p>⚠️ BOTH DIRECTIONS LIVE HERE, which is why they are together. A shape
 * too narrow MISSES (`Instant::now`, `java.nio\n    .file`), and a shape too
 * wide REFUSES correct code (`dataFiles.size()`, `window.now()`,
 * `java.net.URI`). Every case below was added because review measured one or
 * the other.
 */
class IoSeamShapeTest {

  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final String GATE = "scripts/check-io-seam.sh";
  private static final String SCANNER = "scripts/io_seam_scan.py";
  private static final String MAIN = "ingest/src/main/java/binjava/ingest";
  private static final String ADAPTER = "binstore-backends/src/main/java/binjava/binstore/backend";

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
    Files.createDirectories(repo.resolve(ADAPTER));
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
   * ⚠️ A PACKAGE NAME SPLIT ACROSS LINES IS STILL THAT PACKAGE, and nothing
   * pinned it. Review MEASURED dropping {@code _package}'s whitespace tolerance
   * -- the `\s*` either side of each dot -- surviving every case, while
   * {@code java.nio\n    .file.Files.readAllBytes(p)} went from two violations
   * to ZERO. Twelve of the twenty-eight ban entries are packages, so that
   * mutation blinds the gate to the whole package half whenever a formatter
   * wraps a qualified name.
   *
   * <p>⚠️ THE {@code _shape} HALF OF THE SAME CLAIM WAS ALREADY PINNED by
   * {@code aBannedCallSPLITAcrossLinesOrWrittenAsAMETHODREFERENCEIsREFUSED};
   * this is the half the scanner's header asserted and no case covered.
   *
   * ⚠️ EVERY OCCURRENCE IN THE FIXTURE IS SPLIT, and the first draft's was
   * not: it carried an UNSPLIT `(java.nio.file.Path) p` cast on the next line,
   * so the mutant still matched that and the case passed under it. A fixture
   * that also contains the unmutated shape pins nothing -- measured.
   */
  @Test
  void aPACKAGENameSplitAcrossLinesIsSTILLCounted() throws Exception {
    Run r = onFixture("pkg-split", MAIN + "/Splitter.java",
        "package binjava.ingest;\nclass Splitter { Object read() throws Exception {\n"
            + "    return java.nio\n        .file.Files.readAllBytes(java.nio\n"
            + "        .file.Path.of(\"/x\")); } }\n");

    assertThat(r.exit())
        .as("a line break inside a qualified name is not a seam:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("java.nio.file");
  }

  /**
   * ⚠️ A LOWERCASE RECEIVER IS A VALUE, NOT A TYPE, and the anchor that says
   * so was unpinned. Review MEASURED widening it -- {@code [A-Z]} to
   * {@code [A-Za-z_$]} -- surviving every case, while a record accessor called
   * {@code window.now()} became "reaches the real clock directly". A static
   * factory is called on a capitalised name; a value is not.
   *
   * <p>⚠️ THE REAL TREE IS GREEN HERE BY LUCK, not by construction: it holds
   * no lowercase-receiver {@code .now()} today, so only this case separates the
   * two. Its sibling in the miss direction is
   * {@code anIdentifierENDINGInABannedNameIsNotAViolation}.
   */
  @Test
  void aLOWERCASEReceiverIsAValueAndItsNowAccessorIsACCEPTED() throws Exception {
    Run r = onFixture("lowercase-recv", MAIN + "/Window.java",
        "package binjava.ingest;\nrecord Window(long lo, long hi) {\n"
            + "  long now() { return hi; }\n"
            + "  long age(Window window) { return window.now() - lo; } }\n");

    assertThat(r.exit())
        .as("a record accessor named now() reaches nothing:%n%s", r.out())
        .isZero();
  }

  /**
   * ⚠️ A FORMATTER AND A METHOD REFERENCE BOTH USED TO WALK PAST IT, measured
   * by review against the first scanner: the scan was per line, so
   * {@code Instant\n    .now()} was two halves of nothing, and
   * {@code Instant::now} sources the same clock by a syntax the shapes did not
   * admit. The scan now runs over the whole file and every shape accepts
   * {@code ::}.
   */
  @Test
  void aBannedCallSPLITAcrossLinesOrWrittenAsAMETHODREFERENCEIsREFUSED() throws Exception {
    Run split = onFixture("split", MAIN + "/Split.java",
        "package binjava.ingest;\nimport java.time.Instant;\n"
            + "class Split { Instant when() { return Instant\n    .now(); } }\n");
    assertThat(split.exit()).as("a line break is not a seam:%n%s", split.out()).isEqualTo(1);

    Run ref = onFixture("methodref", MAIN + "/Ref.java",
        "package binjava.ingest;\nimport java.time.Instant;\nimport java.util.function.Supplier;\n"
            + "class Ref { Supplier<Instant> s() { return Instant::now; } }\n");
    assertThat(ref.exit()).as("a method reference is not a seam:%n%s", ref.out()).isEqualTo(1);
  }

  /**
   * ⚠️ A METHOD REFERENCE TO A BARE-CALL ENTRY IS STILL A CALL. Review
   * MEASURED {@code System::currentTimeMillis} passing: {@code _shape} has
   * three arms and only the dotted one had learned {@code ::}, while the
   * docstring above it claimed the function as a whole did. The bare arm exists
   * for the static-import form, and the method reference is that form's nearest
   * sibling.
   */
  @Test
  void aMETHODREFERENCEToABareCallEntryIsREFUSED() throws Exception {
    Run r = onFixture("bare-ref", MAIN + "/BareRef.java",
        "package binjava.ingest;\nimport java.util.function.LongSupplier;\n"
            + "class BareRef { LongSupplier t() { return System::currentTimeMillis; } }\n");

    assertThat(r.exit())
        .as("a method reference to the real clock is still the real clock:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("currentTimeMillis(");
  }

  /**
   * ⚠️ AN IDENTIFIER *ENDING* IN A BANNED NAME IS NOT A BANNED CALL, and this
   * is the false-positive direction rather than the miss direction. Review
   * raised it against live code: {@code dataFiles.add(p)} sits in
   * {@code LocalFsBinStore} and was reported as {@code Files.} reaching the
   * filesystem, spared only by the module exemption. MEASURED: without the
   * word anchor this case is the ONLY thing in the suite that reds -- the
   * whole tree happens to keep such identifiers inside the exempt module, so
   * the real-tree case cannot see it.
   *
   * <p>⚠️ IT MATTERS BECAUSE A FALSE REFUSAL IS HOW A GATE GETS WEAKENED. A
   * gate that refuses `dataFiles.size()` is one an author edits rather than
   * obeys, which is the direction non-negotiable 2 forbids.
   */
  @Test
  void anIdentifierENDINGInABannedNameIsNotAViolation() throws Exception {
    Run r = onFixture("suffix", MAIN + "/Names.java",
        "package binjava.ingest;\nimport java.util.List;\n"
            + "class Names { int n(List<String> dataFiles, List<String> files) {\n"
            + "    dataFiles.add(\"x\");\n"
            + "    return dataFiles.size() + files.size(); } }\n");

    assertThat(r.exit())
        .as("an identifier ending in a banned owner name is not a call to it:%n%s", r.out())
        .isZero();
  }

  /**
   * ⚠️ NAMING {@code Clock} IS NOT SOURCING ONE, even written as
   * {@code Clock.class}. Review raised it against an earlier list that banned
   * the whole {@code Clock.} prefix: that form names a type and sources
   * nothing, and it is the one distinction the scanner header, the gate header,
   * AGENTS.md and code-structure.md all claim the gate preserves.
   */
  @Test
  void namingTheClockTYPEIsACCEPTEDWhileSOURCINGOneIsNot() throws Exception {
    Run named = onFixture("clock-type", MAIN + "/Named.java",
        "package binjava.ingest;\nclass Named { Class<?> t() "
            + "{ return java.time.Clock.class; } }\n");
    assertThat(named.exit())
        .as("naming a seam type is the prescribed shape:%n%s", named.out())
        .isZero();

    Run sourced = onFixture("clock-sourced", MAIN + "/Sourced.java",
        "package binjava.ingest;\nclass Sourced { Object t() "
            + "{ return java.time.Clock.systemUTC(); } }\n");
    assertThat(sourced.exit())
        .as("sourcing one is not:%n%s", sourced.out())
        .isEqualTo(1);
  }

  /**
   * ⚠️ READING AN INJECTED CLOCK IS THE PRESCRIBED SHAPE, and the gate refused
   * it. {@code LocalDate.now(clock)} is the JDK's own way to consume a
   * {@code Clock} that arrived through the seam -- review MEASURED the
   * argument-blind entry rejecting it, which is a gate refusing the very thing
   * rule 4 requires, and the surest way to get a gate edited rather than
   * obeyed. The entry is the NO-ARGUMENT form.
   *
   * ⚠️ AND THE METHOD REFERENCE HAS NO PARENS TO BE EMPTY, so pinning arity by
   * literal {@code ()} alone let {@code Instant::now} back through -- measured,
   * and the reason the entry matches both forms.
   */
  @Test
  void READINGAnInjectedClockIsACCEPTEDWhileSOURCINGTheRealOneIsNot() throws Exception {
    Run injected = onFixture("clock-arg", MAIN + "/Reads.java",
        "package binjava.ingest;\nimport java.time.Clock;\nimport java.time.LocalDate;\n"
            + "class Reads { private final Clock clock;\n"
            + "  Reads(Clock clock) { this.clock = clock; }\n"
            + "  LocalDate day() { return LocalDate.now(clock); } }\n");
    assertThat(injected.exit())
        .as("reading the injected clock is what the seam is FOR:%n%s", injected.out())
        .isZero();

    Run real = onFixture("clock-noarg", MAIN + "/Sources.java",
        "package binjava.ingest;\nimport java.time.LocalDate;\n"
            + "class Sources { LocalDate day() { return LocalDate.now(); } }\n");
    assertThat(real.exit())
        .as("the no-argument form sources the real one:%n%s", real.out())
        .isEqualTo(1);

    Run zoned = onFixture("clock-zone", MAIN + "/Zoned.java",
        "package binjava.ingest;\nimport java.time.LocalDateTime;\n"
            + "class Zoned { LocalDateTime t() "
            + "{ return LocalDateTime.now(java.time.ZoneOffset.UTC); } }\n");
    assertThat(zoned.exit())
        .as("a zone overload reads the SYSTEM clock in a named zone, which is still "
            + "the system clock -- review measured it passing when only the "
            + "no-argument form was listed:%n%s", zoned.out())
        .isEqualTo(1);

    Run ref = onFixture("clock-ref", MAIN + "/Ref2.java",
        "package binjava.ingest;\nimport java.time.Instant;\n"
            + "import java.util.function.Supplier;\n"
            + "class Ref2 { Supplier<Instant> s() { return Instant::now; } }\n");
    assertThat(ref.exit())
        .as("and so does the method reference:%n%s", ref.out())
        .isEqualTo(1);
  }

  /**
   * ⚠️ A VALUE TYPE INSIDE A BANNED PACKAGE IS ACCEPTED, AND ITS NEIGHBOUR IS
   * NOT. {@code java.net.URI} is the natural type for an endpoint or an object
   * key and reaches nothing; {@code java.net.URL} opens connections. They share
   * a package AND a prefix, which is why the decision is made on the whole
   * dotted name rather than on the package that matched.
   *
   * ⚠️ IT IS THE SAME ARGUMENT THAT KEEPS {@code java.time} OFF THE PACKAGE
   * LIST, applied inside a package rather than to one -- and review MEASURED
   * the package ban refusing all four of these before the carve-out existed.
   * The OPENING calls stay banned, so allowing {@code URI} opens no route.
   */
  @Test
  void aVALUETYPEInsideABannedPackageIsACCEPTEDAndItsNeighbourIsNot() throws Exception {
    Run value = onFixture("value-type", MAIN + "/Values.java",
        "package binjava.ingest;\nclass Values {\n"
            + "  java.net.URI key(String s) { return java.net.URI.create(s); }\n"
            + "  String enc(String s) { return java.net.URLEncoder.encode(s, "
            + "java.nio.charset.StandardCharsets.UTF_8); } }\n");
    assertThat(value.exit())
        .as("a value type reaches nothing:%n%s", value.out())
        .isZero();

    Run neighbour = onFixture("value-neighbour", MAIN + "/Opens2.java",
        "package binjava.ingest;\nclass Opens2 { Object o() throws Exception "
            + "{ return new java.net.URL(\"http://h/\"); } }\n");
    assertThat(neighbour.exit())
        .as("its neighbour in the same package opens connections:%n%s", neighbour.out())
        .isEqualTo(1);
  }
}
