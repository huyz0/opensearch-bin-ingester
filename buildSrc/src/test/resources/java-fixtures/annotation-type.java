// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class HolderTest {
  @Test void realTest() { assertThat(1).isEqualTo(1); }

  // A JUnit 5 composed annotation declared INSIDE a test class -- the idiom the
  // clusterTest source set will use, and the shape that once hard-blocked both
  // TDD gates on legal code.
  //
  // ⚠️ This fixture does NOT constrain the @interface branch: removing that
  // branch leaves this output unchanged, because `ann` consumes `@interface`
  // before `decl` sees it. It pins the OUTPUT (one real test, no phantom from
  // `brokers()`), not the mechanism. See the note in scripts/java_tests.py.
  @Test
  public @interface ClusterTest { int brokers() default 1; }
}
