// SPDX-License-Identifier: Apache-2.0
package binjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * ⚠️ WHAT A RED RECORD IS HASHED OVER, asserted DIRECTLY.
 *
 * <p>Four review rounds each found one more behaviour of {@code hashable}
 * unconstrained, and the reason was structural rather than carelessness: every
 * assertion about it was made by comparing two 64-hex digests through
 * {@code binding_key}. A digest says only <em>same</em> or <em>different</em>,
 * never <em>why</em> — so each of its behaviours cost a whole fixture file, the
 * coverage was a set of point samples, and each round the set turned out to be
 * one sample short.
 *
 * <p>⚠️ This asserts the transform itself, through the {@code hashable} CLI
 * seam — the idiom this repository already uses for {@code ids}, {@code spans},
 * {@code key} and {@code task-for}. A new behaviour is a table row here, not a
 * new pair of fixtures somewhere else.
 */
class HashableTest {

  @TempDir Path tmp;

  /** description, a, b, and whether the two must hash the same. */
  static List<Object[]> cases() {
    return List.of(
        new Object[] {"a line comment is dropped",
            "int x; // note\n", "int x;\n", true},
        new Object[] {"a block comment is dropped",
            "int /* note */ x;", "int x;", true},
        new Object[] {"a comment must not fuse the tokens either side",
            "int/*c*/x;", "intx;", false},
        new Object[] {"whitespace between tokens is collapsed",
            "int    x;\n\n\n", "int x;", true},
        new Object[] {"whitespace INSIDE a string is not",
            "s(\"a  b\");", "s(\"a b\");", false},
        new Object[] {"a string's content is preserved",
            "s(\"one\");", "s(\"two\");", false},
        new Object[] {"a char literal's content is preserved",
            "c('a');", "c('b');", false},
        new Object[] {"a text block's content is preserved",
            "t(\"\"\"\naaa\n\"\"\");", "t(\"\"\"\nbbb\n\"\"\");", false},
        new Object[] {"whitespace inside a text block is preserved",
            "t(\"\"\"\na  b\n\"\"\");", "t(\"\"\"\na b\n\"\"\");", false},
        new Object[] {"a unicode-escaped quote really delimits a string",
            "s(\\u0022a  b\\u0022);", "s(\\u0022a b\\u0022);", false},
        new Object[] {"a comment inside a string is not a comment",
            "s(\"/* a */\");", "s(\"/* b */\");", false},
        new Object[] {"a text block's LINE STRUCTURE is part of it",
            "t(\"\"\"\naaa\nbbb\n\"\"\");", "t(\"\"\"\naaa bbb\n\"\"\");", false},
        new Object[] {"literal ORDER is part of the code",
            "e(\"one\", \"two\");", "e(\"two\", \"one\");", false});
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void hashableKeepsCodeAndLiteralsAndDropsTheRest(
      String desc, String a, String b, boolean same) throws Exception {
    // ⚠️ Hoisted: AssertJ's varargs `as(...)` is EAGER, so inlining these
    // spawned four python processes per row whether or not the row failed.
    String ha = hashable(a);
    String hb = hashable(b);
    assertThat(ha.equals(hb)).as("%s%n  a -> %s%n  b -> %s", desc, ha, hb).isEqualTo(same);
  }

  private String hashable(String src) throws IOException, InterruptedException {
    Path f = Files.createTempFile(tmp, "h", ".java");
    Files.writeString(f, src);
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Process p =
        new ProcessBuilder("python3", "scripts/tdd_scan.py", "hashable", f.toString())
            .directory(repo.toFile()).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
    return out;
  }
}
