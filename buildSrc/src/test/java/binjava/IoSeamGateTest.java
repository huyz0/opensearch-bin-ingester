// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Non-negotiable 7 as a predicate over files: business logic names no real
 * clock, socket or filesystem.
 *
 * <p>⚠️ IT HAD NO SCRIPT AT ALL, project-wide, and AGENTS.md said so. M5.8 is
 * where the absence became visible — review MEASURED that adding a filesystem
 * read to the membership seam's query survived the whole suite — but the gap
 * was never M5.8's: a test asserting that one class does no I/O covers one
 * class of a hundred and makes the absence look closed.
 *
 * <p>⚠️ THE RULE IS ABOUT SOURCING A REAL INSTANCE, NOT ABOUT NAMING A TYPE.
 * Taking {@code Clock} as a constructor parameter is the PRESCRIBED pattern —
 * code-structure.md rule 4 names it as one of the five seams — so a gate
 * banning the word {@code Clock} would forbid the very shape the rule requires.
 * Same for bytes: {@code java.io.InputStream} is how this project moves bytes
 * and is not I/O, while {@code Files.newInputStream} opens a file. Both
 * non-bans are pinned below, and both cases red under the corresponding
 * widening.
 *
 * <p>⚠️ ONE MODULE IS EXEMPT AND IT IS NAMED RATHER THAN DERIVED FROM
 * {@code implements}. The obvious derivation — a file implementing one of the
 * five seams may do I/O — is wrong here: review MEASURED that eleven
 * {@code src/main} files match {@code implements .*(BinStore|Sequencer|
 * SubscriptionTransport|Membership|Clock)}, {@code LocalSequencer} among them,
 * so that carve-out would be FIVE TIMES WIDER than the one module and would
 * license I/O in the file that holds the commit protocol.
 * {@code binstore-backends} is the object-store adapter, the one module whose
 * JOB is the I/O everything else takes as a seam.
 *
 * <p>⚠️ A DENY-LIST IS NEVER COMPLETE, and the cases below constrain the list
 * rather than pretend otherwise: {@link #everyBANNEDConstructIsREFUSED} fails
 * if any entry is deleted AND if the table here stops covering the scanner's
 * own list. What the gate cannot see is stated in the scanner's header and
 * beside the rule in AGENTS.md.
 */
class IoSeamGateTest {

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

  /**
   * ⚠️ SCRATCH UNDER {@code build/tmp}, never the system temp directory
   * (testing.md rule 17).
   */
  private static Path scratch(String name) throws Exception {
    Path dir = ROOT.resolve("buildSrc/build/tmp/io-seam").resolve(name + "-" + UUID.randomUUID());
    Files.createDirectories(dir);
    return dir;
  }

  /** A repository holding the gate and one clean business-logic file. */
  private static Path fixture(String name) throws Exception {
    Path repo = scratch(name);
    Files.createDirectories(repo.resolve("scripts"));
    Files.createDirectories(repo.resolve(MAIN));
    Files.createDirectories(repo.resolve(ADAPTER));
    Files.copy(ROOT.resolve("scripts/lib.sh"), repo.resolve("scripts/lib.sh"));
    Files.copy(ROOT.resolve(GATE), repo.resolve(GATE));
    Files.copy(ROOT.resolve(SCANNER), repo.resolve(SCANNER));
    repo.resolve(GATE).toFile().setExecutable(true);
    Files.writeString(repo.resolve(MAIN + "/Clean.java"),
        "package binjava.ingest;\nclass Clean { int add(int a, int b) { return a + b; } }\n");
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

  /** A business-logic class whose single method body is {@code body}. */
  private static String logic(String body) {
    return "package binjava.ingest;\nclass Probe { Object go(Object p, Object z, "
        + "Object u, char c) throws Exception { " + body + " return null; } }\n";
  }

  /**
   * ⚠️ THE REAL TREE, not a fixture, and it passes TODAY — which is why this
   * gate lands with no baseline of exemptions. MEASURED before the gate was
   * written: of the tracked {@code src/main} files the rule matches exactly
   * one, {@code LocalFsBinStore}, and that file is the adapter.
   *
   * <p>⚠️ THE COUNT IS ASSERTED, NOT ONLY THE EXIT STATUS. Exit 0 is also what
   * a gate that read nothing returns, and a clean line over a tree it never
   * examined is the shape this project records as worse than a failure. Review
   * MEASURED this assertion as the sole killer of both read-nothing mutations.
   *
   * <p>⚠️ THE ORACLE READS GIT, NOT THE FILESYSTEM, and the first version got
   * that wrong. {@code workspace_files} is tracked-plus-staged, so an
   * UNTRACKED {@code src/main} file made a correct gate red — and this working
   * tree carried exactly such a file while the gate was being written. A walk
   * also cannot see {@code .tmp/}, which holds a reference clone and is the
   * scenario lib.sh says {@code workspace_files} exists to make unreachable.
   */
  /**
   * The composition root's two edge files, exempted BY NAME since M8.4.
   *
   * <p>⚠️ THE ORACLE HAS TO KNOW ABOUT THEM OR IT COUNTS A DIFFERENT SET than
   * the gate does, and the case below would red for a correct gate.
   *
   * <p>⚠️ AND THE LIST IS PINNED BY {@link #theEXEMPTFilesGENUINELYReachPastASeam},
   * so it cannot outlive its reason: if a future change removes the clock or
   * the file read from one of these, that case goes red and the entry must be
   * deleted rather than kept as a standing licence.
   */
  private static final List<String> EXEMPT_FILES = List.of(
      "server/src/main/java/binjava/server/Main.java",
      "server/src/main/java/binjava/server/ConfigFile.java");

  @Test
  void thisRepositoryPassesAndTheGateSaysHowManyFilesItRead() throws Exception {
    Run r = run(ROOT, GATE, "--full");
    assertThat(r.exit()).as("check-io-seam.sh on this tree:%n%s", r.out()).isZero();

    Run listed = run(ROOT, "git", "ls-files", "--", "*/src/main/java/binjava/*.java");
    long files = listed.out().lines()
        .filter(f -> !f.isBlank())
        .filter(f -> !f.startsWith("binstore-backends/"))
        .filter(f -> !EXEMPT_FILES.contains(f))
        .count();
    assertThat(files).as("the oracle must find something to count").isPositive();
    assertThat(r.out())
        .as("the gate must report the work it did -- a counter stuck at 0 prints a clean "
            + "line over a tree it never read%n%s", r.out())
        .contains(files + " business-logic file(s)");
  }

  /**
   * ⚠️ **AN EXEMPTION NOBODY CAN FALSIFY IS A HOLE.** Two files are excused
   * from this gate, and nothing so far says they still need to be. This runs
   * the scanner over exactly those two and asserts that each is reported — so
   * the excuse is measured on every commit, and the day a refactor moves the
   * clock or the file read out of one of them, this case goes red and the entry
   * has to be DELETED rather than carried forward as a standing licence.
   *
   * <p>⚠️ IT IS ALSO THE EVIDENCE THAT THE EXEMPTION IS LOAD-BEARING AT ALL.
   * Without it, "the gate passes" would be equally true of an exemption list
   * naming two files that never reached past a seam in the first place.
   */
  @Test
  void theEXEMPTFilesGENUINELYReachPastASeam() throws Exception {
    for (String file : EXEMPT_FILES) {
      assertThat(ROOT.resolve(file)).as("the exempt list names a file that exists").exists();
      ProcessBuilder pb = new ProcessBuilder("python3", SCANNER).directory(ROOT.toFile());
      pb.redirectErrorStream(true);
      Process p = pb.start();
      p.getOutputStream().write((file + "\n").getBytes(StandardCharsets.UTF_8));
      p.getOutputStream().close();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      p.waitFor();
      assertThat(out)
          .as("%s is exempt from check-io-seam; if the scanner no longer reports it, the "
              + "exemption has outlived its reason and must be removed%n%s", file, out)
          .contains(file);
    }
  }

  /**
   * ⚠️ ONE SAMPLE PER BANNED CONSTRUCT, AND THE TABLE IS CHECKED AGAINST THE
   * SCANNER'S OWN LIST. Review MEASURED the alternative: with a case per
   * construct written by hand, thirteen of sixteen entries could be DELETED
   * from the deny-list with the whole suite green — the gate silently stopping
   * seeing {@code System.currentTimeMillis()}, {@code Clock.systemUTC()}, two
   * of the three socket constructors and both remaining filesystem entry
   * points. Non-negotiable 2 forbids weakening a threshold; a deny-list quietly
   * losing most of itself is the same move.
   *
   * <p>⚠️ THE COVERAGE ASSERTION IS WHAT MAKES IT SELF-MAINTAINING. Adding an
   * entry to {@code BANNED} without a sample here reds this case, so the list
   * and its evidence cannot drift apart.
   *
   * <p>⚠️ THE SAMPLES ARE TEXT, NOT COMPILED. The scanner does not parse Java,
   * so what a sample must be is realistic source; saying so is the honest
   * scope of this case.
   */
  @Test
  void everyBANNEDConstructIsREFUSED() throws Exception {
    Map<String, String> refused = new LinkedHashMap<>();
    refused.put("java.nio.file", "Object t = java.nio.file.Files.exists(p);");
    refused.put("java.nio.channels",
        "Object t = java.nio.channels.AsynchronousFileChannel.open(p);");
    refused.put("java.io", "Object t = new java.io.PrintStream(\"/x\");");
    refused.put("java.util.zip", "Object t = new java.util.zip.ZipFile(\"/x.zip\");");
    refused.put("java.net", "Object t = new java.net.MulticastSocket(1);");
    refused.put("javax.net", "Object t = javax.net.ssl.SSLSocketFactory.getDefault();");
    refused.put("java.util.logging",
        "Object t = new java.util.logging.FileHandler(\"/x\", true);");
    refused.put("java.sql", "Object t = java.sql.DriverManager.getConnection(\"u\");");
    refused.put("java.util.jar", "Object t = new java.util.jar.JarFile(\"/x.jar\");");
    refused.put("java.util.prefs", "Object t = java.util.prefs.Preferences.userRoot();");
    refused.put("javax.sql", "Object t = javax.sql.DataSource.class;");
    refused.put("javax.naming", "Object t = new javax.naming.InitialContext();");
    refused.put(".now()", "Object t = java.time.LocalTime.now();");
    refused.put(".now(...)", "Object t = java.time.LocalDate.now(z);");
    refused.put("currentTimeMillis(", "long t = currentTimeMillis();");
    refused.put("nanoTime(", "long t = nanoTime();");
    refused.put("Clock.system", "Object t = java.time.Clock.systemUTC();");
    refused.put("Clock.tick", "Object t = java.time.Clock.tickSeconds(null);");
    refused.put("new Date(", "Object t = new java.util.Date();");
    refused.put("new GregorianCalendar(", "Object t = new java.util.GregorianCalendar();");
    refused.put("Calendar.getInstance(", "Object t = java.util.Calendar.getInstance();");
    refused.put("new ProcessBuilder(", "Object t = new ProcessBuilder(\"cat\", \"/x\");");
    refused.put(".exec(", "Object t = Runtime.getRuntime().exec(\"x\");");
    refused.put(".getResourceAsStream(", "Object t = getClass().getResourceAsStream(\"/x\");");
    refused.put("getSystemResourceAsStream(",
        "Object t = ClassLoader.getSystemResourceAsStream(\"/x\");");
    refused.put(".toURL(", "Object t = java.net.URI.create(\"http://h/\").toURL();");
    refused.put(".openStream(", "Object t = getClass().getResource(\"/x\").openStream();");
    refused.put(".openConnection(",
        "Object t = getClass().getResource(\"/x\").openConnection();");

    Map<String, String> accepted = new LinkedHashMap<>();
    accepted.put("java.net.URI", "Object t = java.net.URI.create(\"k\");");
    accepted.put("java.net.URLEncoder", "Object t = java.net.URLEncoder.class;");
    accepted.put("java.net.URLDecoder", "Object t = java.net.URLDecoder.class;");
    accepted.put("java.io.IOException", "Object t = java.io.IOException.class;");
    accepted.put("java.io.UncheckedIOException",
        "Object t = java.io.UncheckedIOException.class;");
    accepted.put("java.io.FileNotFoundException",
        "Object t = java.io.FileNotFoundException.class;");
    accepted.put("java.io.InputStream", "Object t = java.io.InputStream.class;");
    accepted.put("java.io.OutputStream", "Object t = java.io.OutputStream.class;");
    accepted.put("java.io.ByteArrayInputStream",
        "Object t = new java.io.ByteArrayInputStream(new byte[0]);");
    accepted.put("java.io.ByteArrayOutputStream",
        "Object t = new java.io.ByteArrayOutputStream();");
    accepted.put("java.io.FilterInputStream", "Object t = java.io.FilterInputStream.class;");
    accepted.put("java.io.Closeable", "Object t = java.io.Closeable.class;");
    accepted.put("java.io.Flushable", "Object t = java.io.Flushable.class;");
    accepted.put("java.io.DataInputStream", "Object t = java.io.DataInputStream.class;");
    accepted.put("java.io.DataOutputStream", "Object t = java.io.DataOutputStream.class;");
    accepted.put("java.io.Serializable", "Object t = java.io.Serializable.class;");
    accepted.put("java.util.zip.CRC32", "Object t = new java.util.zip.CRC32();");
    accepted.put("java.util.zip.CRC32C", "Object t = new java.util.zip.CRC32C();");
    accepted.put("java.util.zip.Adler32", "Object t = new java.util.zip.Adler32();");
    accepted.put("java.util.zip.Checksum", "Object t = java.util.zip.Checksum.class;");

    Run listed = run(ROOT, "python3", "-c",
        "import sys; sys.path.insert(0, 'scripts'); import io_seam_scan; "
            + "print('\\n'.join(io_seam_scan.ALL_ENTRIES))");
    assertThat(listed.exit()).as("could not read the list:%n%s", listed.out()).isZero();
    java.util.List<String> table = new java.util.ArrayList<>(refused.keySet());
    accepted.keySet().forEach(k -> table.add("allow:" + k));
    assertThat(listed.out().lines().filter(l -> !l.isBlank()).toList())
        .as("every entry in the scanner's list -- the CARVE-OUT included -- needs a sample "
            + "here, and every sample needs an entry. Review MEASURED a carve-out widened "
            + "in silence when only the ban half was covered")
        .containsExactlyInAnyOrderElementsOf(table);

    Run control = onFixture("control", MAIN + "/Probe.java", logic(""));
    assertThat(control.exit())
        .as("PREMISE: the fixture's own signature must name nothing banned, or every "
            + "sample below is refused before its body is read -- review measured exactly "
            + "that%n%s", control.out())
        .isZero();

    for (Map.Entry<String, String> e : refused.entrySet()) {
      Run r = onFixture("banned", MAIN + "/Probe.java", logic(e.getValue()));
      assertThat(r.exit()).as("%s must be REFUSED:%n%s", e.getKey(), r.out()).isEqualTo(1);
      assertThat(r.out()).as("the gate must name the construct it refused")
          .contains(e.getKey());
    }
    for (Map.Entry<String, String> e : accepted.entrySet()) {
      Run r = onFixture("allowed", MAIN + "/Probe.java", logic(e.getValue()));
      assertThat(r.exit())
          .as("%s reaches nothing and must be ACCEPTED:%n%s", e.getKey(), r.out())
          .isZero();
    }
  }


  /**
   * Taking {@code Clock} as a seam is ACCEPTED.
   *
   * <p>⚠️ THE CASE THAT STOPS THIS BEING A BLANKET BAN, and without it the
   * cheapest way to pass the case above is to forbid the word {@code Clock} —
   * which would refuse the pattern code-structure.md rule 4 REQUIRES, and red
   * the sequencer module, where every clock arrives this way. ⚠️ AND
   * {@code java.time} IS THE ONE FAMILY THE PACKAGE BAN CANNOT TAKE, for
   * exactly that reason -- so the clock is named by construct, and
   * {@link #namingTheClockTYPEIsACCEPTEDWhileSOURCINGOneIsNot} is what keeps
   * that naming from widening back to the type.
   */
  @Test
  void takingAClockAsASeamParameterIsACCEPTED() throws Exception {
    Run r = onFixture("clock-seam", MAIN + "/Seamed.java",
        "package binjava.ingest;\nimport java.time.Clock;\nimport java.time.Instant;\n"
            + "class Seamed { private final Clock clock;\n"
            + "  Seamed(Clock clock) { this.clock = clock; }\n"
            + "  Instant when() { return clock.instant(); } }\n");

    assertThat(r.exit()).as("a Clock parameter is the prescribed shape:%n%s", r.out()).isZero();
  }

  /**
   * ⚠️ THE SAME CALL, THE TWO MODULES — which is what makes the exemption
   * load-bearing rather than a hole. MEASURED: deleting the exemption reds the
   * first of these and the real tree; widening it to every module reds the
   * second, and five cases in all.
   */
  @Test
  void aFILESYSTEMReadIsACCEPTEDInTheAdapterAndREFUSEDOutsideIt() throws Exception {
    String body = "package binjava.binstore.backend;\nimport java.nio.file.Files;\n"
        + "import java.nio.file.Path;\n"
        + "class Reader { byte[] all(Path p) throws Exception { return Files.readAllBytes(p); } }\n";

    Run adapter = onFixture("fs-adapter", ADAPTER + "/Reader.java", body);
    assertThat(adapter.exit())
        .as("binstore-backends IS the object-store adapter:%n%s", adapter.out())
        .isZero();

    Run logic = onFixture("fs-logic", MAIN + "/Reader.java",
        body.replace("binjava.binstore.backend", "binjava.ingest"));
    assertThat(logic.exit())
        .as("the identical call outside the adapter must FAIL:%n%s", logic.out())
        .isEqualTo(1);
    assertThat(logic.out()).contains("java.nio.file");
  }

  /**
   * ⚠️ THE EXEMPTION IS ANCHORED TO THE PATH'S START, and review MEASURED that
   * unanchoring it survives every other case — the adapter fixture's path
   * begins with the module name either way. Unanchored, any future path merely
   * CONTAINING {@code binstore-backends} leaves non-negotiable 7 silently.
   */
  @Test
  void theExemptionIsANCHOREDToThePathsSTART() throws Exception {
    Run r = onFixture("anchor",
        "ingest/src/main/java/binjava/ingest/binstore-backends/Sneaky.java",
        "package binjava.ingest;\nimport java.nio.file.Files;\nimport java.nio.file.Path;\n"
            + "class Sneaky { byte[] all(Path p) throws Exception "
            + "{ return Files.readAllBytes(p); } }\n");

    assertThat(r.exit())
        .as("only a path STARTING with the adapter module is exempt:%n%s", r.out())
        .isEqualTo(1);
  }

  /**
   * ⚠️ BYTES ARE NOT I/O. {@code java.io.InputStream} is how every segment in
   * this project moves, so a gate keyed on the {@code java.io} package would
   * refuse `format` wholesale and be deleted within a commit. What opens a file
   * is {@code Files.newInputStream}, and only that is banned. MEASURED: adding
   * the package to the deny-list reds this case and the real tree.
   */
  @Test
  void handlingAnInputStreamIsACCEPTEDWhileOPENINGOneIsNot() throws Exception {
    Run bytes = onFixture("bytes-ok", MAIN + "/Bytes.java",
        "package binjava.ingest;\nimport java.io.ByteArrayInputStream;\nimport java.io.InputStream;\n"
            + "class Bytes { InputStream of(byte[] b) { return new ByteArrayInputStream(b); } }\n");
    assertThat(bytes.exit())
        .as("moving bytes is not touching a socket, clock or store:%n%s", bytes.out())
        .isZero();

    Run opens = onFixture("bytes-bad", MAIN + "/Opens.java",
        "package binjava.ingest;\nimport java.io.InputStream;\nimport java.nio.file.Files;\n"
            + "import java.nio.file.Path;\n"
            + "class Opens { InputStream of(Path p) throws Exception "
            + "{ return Files.newInputStream(p); } }\n");
    assertThat(opens.exit()).as("opening a file must FAIL:%n%s", opens.out()).isEqualTo(1);
  }






  /**
   * ⚠️ THE SUCCESS LINE'S CONSTRUCT COUNT IS DERIVED FROM THE LIST, not typed
   * beside it. Review MEASURED a hardcoded count surviving every case -- the
   * gate then announcing one number over a list of another, in the same
   * sentence whose FILE count this suite deliberately pins.
   */
  @Test
  void theSuccessLineCountsTheConstructsTheListACTUALLYHolds() throws Exception {
    Run listed = run(ROOT, "python3", "-c",
        "import sys; sys.path.insert(0, 'scripts'); import io_seam_scan; "
            + "print(len(io_seam_scan.BAN_ENTRIES))");
    assertThat(listed.exit()).as("could not read the list:%n%s", listed.out()).isZero();
    String entries = listed.out().trim();

    Run r = run(ROOT, GATE, "--full");
    assertThat(r.out())
        .as("the number in the success line must be the list's own length%n%s", r.out())
        .contains("none of the " + entries + " banned constructs");
  }




  /**
   * ⚠️ A FILE THE SCANNER CANNOT READ IS REPORTED, NOT SKIPPED, and review
   * MEASURED that dropping the report left every case green — the gate then
   * prints a clean line over a file it never opened, which is the failure mode
   * this suite's real-tree case calls worse than a failure.
   */
  @Test
  void aFileTheScannerCannotREADIsREPORTEDNotSkipped() throws Exception {
    Path repo = fixture("unreadable");
    Path f = repo.resolve(MAIN + "/Locked.java");
    Files.writeString(f, "package binjava.ingest;\nclass Locked { }\n");
    run(repo, "git", "add", "-A");
    assertThat(f.toFile().setReadable(false))
        .as("this case needs an unreadable file; running as root makes that impossible")
        .isTrue();

    Run r = run(repo, GATE);
    f.toFile().setReadable(true);

    assertThat(r.exit()).as("a file that could not be read is not a clean file:%n%s", r.out())
        .isEqualTo(1);
    assertThat(r.out()).contains("unreadable");
  }

  /**
   * ⚠️ A MISSING SCANNER FAILS LOUDLY, and review MEASURED the branch that says
   * so as unreachable by any case — a gate whose engine is gone must not report
   * a clean tree.
   */
  @Test
  void aMISSINGScannerFAILSTheGateRatherThanPassing() throws Exception {
    Path repo = fixture("no-scanner");
    Files.writeString(repo.resolve(MAIN + "/Any.java"),
        "package binjava.ingest;\nclass Any { }\n");
    run(repo, "git", "add", "-A");
    Files.delete(repo.resolve(SCANNER));

    Run r = run(repo, GATE);

    assertThat(r.exit()).as("no engine is not a clean tree:%n%s", r.out()).isEqualTo(1);
    assertThat(r.out()).contains("scanner");
  }

  /**
   * A change touching NO business-logic file passes.
   *
   * <p>⚠️ MEASURED ON THIS GATE'S OWN COMMIT, which touches scripts, the
   * harness suite and three documents and no {@code src/main} file at all.
   * lib.sh sets {@code pipefail} and {@code grep -v} exits 1 when it filters
   * everything away, so the first version died as "the scanner failed" over an
   * empty list — a gate reporting a malfunction where it should report nothing
   * to do, on the commit shape that is the common case.
   */
  @Test
  void aChangeTouchingNOBusinessLogicFilePASSES() throws Exception {
    Path repo = fixture("empty");
    Files.writeString(repo.resolve("scripts/notes.md"), "no java here\n");
    run(repo, "git", "add", "-A");
    run(repo, "git", "commit", "-qm", "base");
    Files.writeString(repo.resolve("scripts/notes.md"), "still no java here\n");
    run(repo, "git", "add", "-A");

    Run r = run(repo, GATE);

    assertThat(r.exit()).as("nothing to read is not a failure:%n%s", r.out()).isZero();
    assertThat(r.out()).contains("0 business-logic file(s)");
  }

  /**
   * ⚠️ THE MODE IS PRINTED. A {@code delta} pass is a weaker statement than a
   * {@code full} one and must not read like one — lib.sh says so in as many
   * words, and non-negotiable 4 is why.
   */
  @Test
  void theGateSaysWhichSCOPEItRan() throws Exception {
    Path repo = fixture("scope");
    assertThat(run(repo, GATE).out()).contains("changed files only");
    assertThat(run(repo, GATE, "--full").out()).contains("full tree");
  }




  /**
   * ⚠️ THE CARVE-OUT IS WHAT MAKES `java.io` BANNABLE AT ALL, and getting it
   * wrong is what let a sibling walk past. Review MEASURED
   * {@code new java.io.PrintStream("/tmp/x")} passing a list that named
   * {@code java.io.PrintWriter} -- three class-name prefixes wearing a package
   * ban's clothes, under a header claiming the I/O half was closed by
   * construction. The package is banned now, and the eight byte and exception
   * types this tree actually uses are named in {@code ALLOWED}.
   *
   * ⚠️ BOTH DIRECTIONS OR NEITHER: without the accepted half the cheapest way
   * to pass is to drop the package, and without the refused half the carve-out
   * can swallow the package whole.
   */
  @Test
  void theBYTETypesInABannedPackageAreACCEPTEDWhileItsFILETypesAreNot() throws Exception {
    Run bytes = onFixture("io-bytes", MAIN + "/Bytes2.java",
        "package binjava.ingest;\nimport java.io.ByteArrayOutputStream;\n"
            + "import java.io.IOException;\nimport java.io.InputStream;\n"
            + "class Bytes2 { InputStream f(byte[] b) throws IOException {\n"
            + "    ByteArrayOutputStream out = new ByteArrayOutputStream();\n"
            + "    out.write(b);\n"
            + "    return new java.io.ByteArrayInputStream(out.toByteArray()); } }\n");
    assertThat(bytes.exit())
        .as("the byte types are how every segment in this project moves:%n%s", bytes.out())
        .isZero();

    Run file = onFixture("io-file", MAIN + "/Streams.java",
        "package binjava.ingest;\nclass Streams { Object f() throws Exception "
            + "{ return new java.io.PrintStream(\"/tmp/x\"); } }\n");
    assertThat(file.exit())
        .as("PrintStream is the sibling that walked past a list naming PrintWriter:%n%s",
            file.out())
        .isEqualTo(1);
  }

  /**
   * ⚠️ A SUBPROCESS IS ITS OWN KIND, and the reach is {@code .exec(} rather
   * than the handle. Review MEASURED {@code Runtime.getRuntime()} refused as
   * "reaches the filesystem" while {@code availableProcessors()} -- sizing a
   * pool by core count, reaching nothing -- was refused with it. A gate that
   * refuses that is one an author edits rather than obeys.
   */
  @Test
  void GETTINGTheRuntimeIsACCEPTEDWhileEXECUTINGThroughItIsNot() throws Exception {
    Run sizing = onFixture("rt-size", MAIN + "/Sizing.java",
        "package binjava.ingest;\nclass Sizing { int n() "
            + "{ return Runtime.getRuntime().availableProcessors(); } }\n");
    assertThat(sizing.exit())
        .as("core count reaches nothing:%n%s", sizing.out())
        .isZero();

    Run exec = onFixture("rt-exec", MAIN + "/Exec.java",
        "package binjava.ingest;\nclass Exec { Object go() throws Exception "
            + "{ return Runtime.getRuntime().exec(\"cat /etc/hostname\"); } }\n");
    assertThat(exec.exit())
        .as("executing through it reaches a subprocess:%n%s", exec.out())
        .isEqualTo(1);
    assertThat(exec.out()).contains("a subprocess");
  }
}
