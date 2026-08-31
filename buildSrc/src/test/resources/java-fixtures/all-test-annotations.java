// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.*;
// @TestInstance is a real JUnit annotation whose simple name STARTS WITH "Test"
// but is not one. Without TEST_ANN's trailing $ anchor it arms the scanner and
// the next signature becomes a phantom test.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AnnotationsTest {
  private int helper(int n) { return n; }

  @Test void plain() { assertThat(1).isEqualTo(1); }
  @RepeatedTest(3) void repeated() { assertThat(2).isEqualTo(2); }
  @TestFactory java.util.stream.Stream<Object> factory() { return null; }
  @TestTemplate void template() { assertThat(4).isEqualTo(4); }
  @ParameterizedTest void parameterised(int n) { assertThat(n).isEqualTo(n); }
}
